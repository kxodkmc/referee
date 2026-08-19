//! 实例抽象与多实例管理 — 生命周期 + 有界治理 + 请求路由
//!
//! **职责**：实例生命周期（create / chat / interrupt / stop / snapshot / 恢复回放）
//! 与多实例有界管理（创建 / 列出 / 查询 / 停止 / 移除）。**transport-agnostic**：
//! 只暴露业务方法，由传输层（TCP / 未来 HTTP）调用。
//!
//! 实例隔离硬约束：每个实例持有独立 `AgentRuntime`（内含独立 `Engine` / 会话表 /
//! 模板注册与工具集）；`InstanceSpec.tools.fs.root` 即实例工作区根，实例间文件
//! 视图互不可见。

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tokio::sync::RwLock;

use referee_ai::engine::{ChatHandle, Engine, EngineStartError};
use referee_ai::provider::Message;
use referee_ai::session::{ChatPayload, SessionId};
use referee_ai::tool::{ToolExecutor, ToolRegistry};
use referee_agent::tool::fs_common::FsConfig;
use referee_agent::tool::read::ReadToolConfig;
use referee_agent::{AgentRuntime, InMemoryArtifactStore};

use crate::persist::{into_dyn, BrokenEntry, PersistStore, RecoveryResult};
use crate::protocol::{InstanceId, InstanceInfo, InstanceSpec, InstanceState, ProviderConfig, ServerError};

/// 错误码常量（传输层 JSON-RPC 使用）
pub mod err {
    pub const ERR_INSTANCE_NOT_FOUND: i32 = -32000;
    pub const ERR_INSTANCE_FULL: i32 = -32001;
    pub const ERR_SESSION_BUSY: i32 = -32002;
    pub const ERR_INTERNAL: i32 = -32003;
    pub const ERR_INVALID_SPEC: i32 = -32004;
}

use err as ErrorCode;

/// 实例状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceStatus {
    Running,
    Stopped,
}

/// 一个可寻址、可治理、相互隔离的智能体实例
#[derive(Clone)]
pub struct Instance {
    id: InstanceId,
    spec: InstanceSpec,
    runtime: AgentRuntime,
    status: Arc<RwLock<InstanceStatus>>,
    created_at: SystemTime,
}

impl std::fmt::Debug for Instance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Instance")
            .field("id", &self.id)
            .field("model", &self.spec.agent.model)
            .field("sessions", &self.runtime.session_count())
            .field("status", &self.status.try_read().map(|s| *s).unwrap_or(InstanceStatus::Running))
            .finish()
    }
}

impl Instance {
    /// 由规格构造实例（G3 配置装载关键接线）
    ///
    /// `provider` 为已构造的 LLM 提供者（由管理器解析 `spec.provider` 或注入）；
    /// `log_sink` 为可插拔会话落盘 sink（None 时不落盘）；`global_budget` 为
    /// 系统级共享 Token 计数器（经 `Engine::with_global_budget` 注入）。
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        spec: InstanceSpec,
        id: InstanceId,
        provider: Arc<dyn referee_ai::provider::LLMProvider>,
        log_sink: Option<Arc<dyn referee_ai::session::SessionLogSink>>,
        global_budget: Arc<AtomicU64>,
    ) -> Result<Self, ServerError> {
        // 1. AgentDefinition → 绑定模板 → 系统提示词
        let system_prompt = bind_agent(&spec)?;

        // 3. 组装引擎配置（注入系统提示词 + 落盘 sink）
        let mut engine_config = spec.engine.clone();
        engine_config.session.default_system_prompt = {
            if system_prompt.is_empty() {
                None
            } else {
                Some(system_prompt.clone())
            }
        };
        engine_config.session.log_sink = log_sink;

        let engine = Engine::new(provider, engine_config)
            .with_tools(ToolRegistry::with_defaults(), ToolExecutor::with_defaults())
            .with_global_budget(global_budget);

        // 4. 业务运行时
        let mut runtime = AgentRuntime::new(engine);

        // 5. 注册工具（按 spec.tools；fs.root 即实例工作区根，实例隔离关键点）
        register_tools(&mut runtime, &spec)?;

        Ok(Self {
            id,
            spec,
            runtime,
            status: Arc::new(RwLock::new(InstanceStatus::Running)),
            created_at: SystemTime::now(),
        })
    }

    /// 发起一轮流式 Chat（返回句柄，由调用方消费 chunk 流）
    pub fn chat(
        &self,
        session_id: SessionId,
        payload: ChatPayload,
    ) -> Result<ChatHandle, EngineStartError> {
        self.runtime.chat_stream(session_id, payload)
    }

    /// 中断指定会话当前回合
    pub fn interrupt(&self, session_id: SessionId) -> bool {
        self.runtime.interrupt(session_id)
    }

    /// 回放已确认会话事实（崩溃恢复用）
    pub fn replay_history(&self, session_id: SessionId, msgs: Vec<Message>) -> Result<usize, String> {
        self.runtime
            .restore_session_history(session_id, msgs)
            .map_err(|e| e.to_string())
    }

    /// 停止实例：取消全部在飞回合 + 置 Stopped（实例仍保留供观测）
    pub async fn stop(&self) {
        let sessions: Vec<SessionId> = self.runtime.list_sessions();
        for sid in sessions {
            self.runtime.interrupt(sid);
        }
        *self.status.write().await = InstanceStatus::Stopped;
    }

    /// 观测快照
    pub async fn snapshot(&self) -> InstanceInfo {
        InstanceInfo {
            id: self.id.clone(),
            model: self.spec.agent.model.clone(),
            state: match *self.status.read().await {
                InstanceStatus::Running => InstanceState::Running,
                InstanceStatus::Stopped => InstanceState::Stopped,
            },
            sessions: self.runtime.session_count(),
            max_sessions: self.spec.engine.max_sessions,
            consumed_tokens: self.runtime.total_consumed_tokens(),
            cache_entries: self.runtime.cache_len(),
            created_at: iso8601(self.created_at),
        }
    }

    /// 会话列表（instance.sessions）
    pub fn session_infos(&self) -> Vec<crate::protocol::SessionInfo> {
        use referee_ai::engine::SessionPhase;
        self.runtime
            .list_sessions()
            .into_iter()
            .filter_map(|sid| {
                let snap = self.runtime.session_info(sid)?;
                Some(crate::protocol::SessionInfo {
                    id: sid.to_string(),
                    messages: snap.history_len,
                    phase: match snap.state {
                        SessionPhase::Idle => "idle",
                        SessionPhase::Thinking => "thinking",
                        SessionPhase::AwaitingCalls => "awaiting_calls",
                    }
                    .to_string(),
                    consumed_tokens: snap.consumed_tokens,
                })
            })
            .collect()
    }
}

/// 绑定 Agent 模板 → 系统提示词文本
fn bind_agent(spec: &InstanceSpec) -> Result<String, ServerError> {
    let templates = referee_agent::TemplateRegistry::with_builtins();
    let vars: Vec<(String, String)> = spec.template_vars.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let var_refs: Vec<(&str, &str)> = vars.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let bound = Arc::new(spec.agent.clone())
        .bind_with(Some(&templates), &var_refs)
        .map_err(|e| ServerError::new(ErrorCode::ERR_INVALID_SPEC, format!("template bind error: {e}")))?;
    let text = bound
        .system_sections
        .iter()
        .map(|s| s.text.trim())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    Ok(text)
}

/// 按 spec.tools 注册实例工具（实例隔离关键点）
fn register_tools(
    runtime: &mut AgentRuntime,
    spec: &InstanceSpec,
) -> Result<(), ServerError> {
    if let Some(fs) = &spec.tools.fs {
        let root = fs.root.clone().map(PathBuf::from);
        runtime
            .register_read_tool(ReadToolConfig {
                default_limit_chars: fs.default_limit_chars,
                max_limit_chars: 20_000,
                max_file_bytes: fs.max_file_bytes,
                root: root.clone(),
            })
            .map_err(|e| ServerError::new(ErrorCode::ERR_INVALID_SPEC, e.to_string()))?;
        runtime
            .register_fs_write_tools(FsConfig {
                max_file_bytes: fs.max_file_bytes,
                root,
            })
            .map_err(|e| ServerError::new(ErrorCode::ERR_INVALID_SPEC, e.to_string()))?;
    }
    if spec.tools.artifact {
        let store = Arc::new(InMemoryArtifactStore::with_defaults());
        *runtime = runtime.clone().with_artifact_store(store);
        runtime
            .register_artifact_tools()
            .map_err(|e| ServerError::new(ErrorCode::ERR_INVALID_SPEC, e.to_string()))?;
    }
    Ok(())
}

/// 有界实例管理器
#[derive(Clone)]
pub struct InstanceManager {
    instances: Arc<DashMap<InstanceId, Instance>>,
    config: InstanceManagerConfig,
    global_budget: Arc<AtomicU64>,
    persist: Option<PersistStore>,
}

/// 实例管理器配置
#[derive(Debug, Clone)]
pub struct InstanceManagerConfig {
    pub max_instances: usize,
    pub max_sessions_per_instance: usize,
    /// 系统级总预算（0 = 无限制）
    pub global_budget_limit: u64,
}

impl Default for InstanceManagerConfig {
    fn default() -> Self {
        Self {
            max_instances: 64,
            max_sessions_per_instance: 100,
            global_budget_limit: 0,
        }
    }
}

impl InstanceManager {
    pub fn new(config: InstanceManagerConfig) -> Self {
        Self {
            instances: Arc::new(DashMap::new()),
            config,
            global_budget: Arc::new(AtomicU64::new(0)),
            persist: None,
        }
    }

    /// 注入持久化后端（启用崩溃恢复 + 会话落盘）
    pub fn with_persist(mut self, persist: PersistStore) -> Self {
        self.persist = Some(persist);
        self
    }

    /// 创建实例（有界：满则 ERR_INSTANCE_FULL；重名则 ERR_INVALID_SPEC）
    pub fn create(&self, spec: InstanceSpec) -> Result<InstanceId, ServerError> {
        self.create_with_provider(spec, None)
    }

    /// 创建实例并注入 provider（测试 / 编程式装配用；`None` 时按 `spec.provider` 解析）
    pub fn create_with_provider(
        &self,
        spec: InstanceSpec,
        provider: Option<Arc<dyn referee_ai::provider::LLMProvider>>,
    ) -> Result<InstanceId, ServerError> {
        let id = match &spec.id {
            Some(s) => {
                InstanceId::new(s).map_err(|e| ServerError::new(ErrorCode::ERR_INVALID_SPEC, e.to_string()))?
            }
            None => InstanceId::generate(),
        };
        if self.instances.contains_key(&id) {
            return Err(ServerError::new(
                ErrorCode::ERR_INVALID_SPEC,
                format!("instance id '{id}' already exists"),
            ));
        }
        if self.instances.len() >= self.config.max_instances {
            return Err(ServerError::new(
                ErrorCode::ERR_INSTANCE_FULL,
                format!("max instances ({}) reached", self.config.max_instances),
            ));
        }

        let mut spec = spec;
        if self.config.global_budget_limit > 0 {
            spec.engine.budget.global_limit = self.config.global_budget_limit;
        }

        let provider = match provider {
            Some(p) => p,
            None => build_provider(&spec.provider)?,
        };
        let log_sink = self
            .persist
            .as_ref()
            .map(|p| into_dyn(p.instance_sink(id.clone())));
        let instance = Instance::create(spec.clone(), id.clone(), provider, log_sink, self.global_budget.clone())?;

        if let Some(p) = &self.persist {
            p.save_instance(&id, &spec)
                .map_err(|e| ServerError::new(ErrorCode::ERR_INTERNAL, format!("persist instance: {e}")))?;
        }
        self.instances.insert(id.clone(), instance);
        Ok(id)
    }

    /// 列出全部实例（观测快照）
    pub async fn list(&self) -> Vec<InstanceInfo> {
        let mut out = Vec::with_capacity(self.instances.len());
        for entry in self.instances.iter() {
            out.push(entry.value().snapshot().await);
        }
        out
    }

    /// 查询单个实例（克隆句柄）
    pub fn get(&self, id: &InstanceId) -> Result<Instance, ServerError> {
        self.instances
            .get(id)
            .map(|e| e.clone())
            .ok_or_else(|| ServerError::new(ErrorCode::ERR_INSTANCE_NOT_FOUND, format!("instance '{id}' not found")))
    }

    /// 停止并移除实例
    pub async fn remove(&self, id: &InstanceId) -> Result<(), ServerError> {
        let instance = self.get(id)?;
        instance.stop().await;
        self.instances.remove(id);
        if let Some(p) = &self.persist {
            p.remove_instance(id)
                .map_err(|e| ServerError::new(ErrorCode::ERR_INTERNAL, format!("remove persist: {e}")))?;
        }
        Ok(())
    }

    /// 遍历全部实例 id（崩溃恢复用，无锁持跨 await）
    pub fn iter(&self) -> impl Iterator<Item = InstanceId> + '_ {
        self.instances.iter().map(|e| e.key().clone())
    }

    /// 崩溃恢复：重建实例 + 回放已确认会话事实
    ///
    /// 不可恢复的实例 / 会话进入 broken 清单，不阻塞启动。
    pub async fn recover(&self, persist: &PersistStore) -> RecoveryResult {
        let mut result = RecoveryResult::default();
        let (specs, mut broken) = match persist.load_instances() {
            Ok(x) => x,
            Err(e) => {
                result.broken.push(BrokenEntry {
                    path: "instances".into(),
                    reason: e.to_string(),
                });
                return result;
            }
        };
        result.broken.append(&mut broken);

        for (id, spec) in specs {
            match self.create(spec) {
                Ok(_) => result.recovered_instances += 1,
                Err(e) => result.broken.push(BrokenEntry {
                    path: id.to_string(),
                    reason: e.message,
                }),
            }
        }

        let ids: Vec<InstanceId> = self.iter().collect();
        for id in ids {
            let instance = match self.get(&id) {
                Ok(i) => i,
                Err(_) => continue,
            };
            let (sessions, mut sb) = match persist.load_instance_sessions(&id) {
                Ok(x) => x,
                Err(e) => {
                    result.broken.push(BrokenEntry {
                        path: id.to_string(),
                        reason: e.to_string(),
                    });
                    continue;
                }
            };
            result.broken.append(&mut sb);
            for (session_id, msgs) in sessions {
                match instance.replay_history(session_id, msgs) {
                    Ok(n) => result.recovered_sessions += n,
                    Err(e) => result.broken.push(BrokenEntry {
                        path: format!("{}/{session_id}", id.as_str()),
                        reason: e,
                    }),
                }
            }
        }
        result
    }
}

/// 按厂商配置构造 LLM 提供者（feature 门控；未启用或不可用 → 显式 ERR_INVALID_SPEC）
fn build_provider(
    cfg: &ProviderConfig,
) -> Result<Arc<dyn referee_ai::provider::LLMProvider>, ServerError> {
    match cfg {
        #[cfg(feature = "deepseek")]
        ProviderConfig::DeepSeek {
            api_key,
            base_url,
            model,
        } => {
            use referee_ai::provider::deepseek::{DeepSeekConfig, DeepSeekModel, DeepSeekProvider};
            let m = match model.as_deref() {
                Some(s) if s.contains("pro") => DeepSeekModel::V4Pro,
                _ => DeepSeekModel::V4Flash,
            };
            let mut c = DeepSeekConfig::new(api_key.clone());
            if let Some(url) = base_url {
                c = c.with_base_url(url.clone());
            }
            DeepSeekProvider::new(m, c)
                .map(|p| Arc::new(p) as Arc<dyn referee_ai::provider::LLMProvider>)
                .map_err(|e| ServerError::new(ErrorCode::ERR_INVALID_SPEC, e.to_string()))
        }
        #[cfg(feature = "xiaomi")]
        ProviderConfig::XiaoMi { api_key, base_url } => {
            use referee_ai::provider::xiaomi::{XiaomiConfig, XiaomiModel, XiaomiProvider};
            let mut c = XiaomiConfig::new(api_key.clone());
            if let Some(url) = base_url {
                c = c.with_base_url(url.clone());
            }
            XiaomiProvider::new(XiaomiModel::MimoV25Pro, c)
                .map(|p| Arc::new(p) as Arc<dyn referee_ai::provider::LLMProvider>)
                .map_err(|e| ServerError::new(ErrorCode::ERR_INVALID_SPEC, e.to_string()))
        }
        #[cfg(not(feature = "deepseek"))]
        ProviderConfig::DeepSeek { .. } => Err(ServerError::new(
            ErrorCode::ERR_INVALID_SPEC,
            "deepseek feature not enabled",
        )),
        #[cfg(not(feature = "xiaomi"))]
        ProviderConfig::XiaoMi { .. } => Err(ServerError::new(
            ErrorCode::ERR_INVALID_SPEC,
            "xiaomi feature not enabled",
        )),
        // OpenAI 适配器在 base 中为 pub(crate)，未暴露给服务层；显式报错而非静默
        ProviderConfig::OpenAI { .. } => Err(ServerError::new(
            ErrorCode::ERR_INVALID_SPEC,
            "openai provider adapter not exposed to server layer",
        )),
    }
}

/// SystemTime → ISO 8601（UTC，`YYYY-MM-DDTHH:MM:SSZ`），零依赖实现（民用历法）
fn iso8601(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let days = (d.as_secs() / 86_400) as i64;
    let secs = d.as_secs() % 86_400;
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}Z")
}