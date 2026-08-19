//! 厂商注册表 — 多 Provider 的发现、路由与健康检查
//!
//! `ProviderRegistry` 是 `Arc<dyn LLMProvider>` 的有界集合：按 `ProviderId`
//! 注册 / 查找 / 列举 / 批量健康检查。不感知 HTTP、不感知配置加载——
//! 调用方负责构造 Provider 实例后注册进来。
//!
//! 设计约束：
//! - 线程安全：`DashMap` + `Arc<dyn LLMProvider>`，读路径无锁
//! - 轻量：不拥有配置、不管理连接池；注册的是已构造好的 Provider 句柄
//! - 健康检查并行：`health_check_all` 同时探活全部 Provider，超时可控

use std::sync::Arc;

use dashmap::DashMap;
use tokio::time::Duration;

use crate::provider::{LLMProvider, LlmError, ProviderId};

/// 厂商注册错误
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ProviderRegistryError {
    /// 已存在同名 Provider（重复注册）
    #[error("provider already registered: {0}")]
    AlreadyExists(String),
}

/// 单个厂商的健康状态
#[derive(Debug, Clone)]
pub struct ProviderStatus {
    pub id: ProviderId,
    pub healthy: bool,
    pub error: Option<String>,
}

/// 厂商注册表 — 多 Provider 发现与路由
#[derive(Default)]
pub struct ProviderRegistry {
    providers: DashMap<ProviderId, Arc<dyn LLMProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册 Provider（重复 ID 返回错误）
    pub fn register(
        &self,
        provider: Arc<dyn LLMProvider>,
    ) -> Result<(), ProviderRegistryError> {
        let id = provider.id();
        if self.providers.contains_key(&id) {
            return Err(ProviderRegistryError::AlreadyExists(id.to_string()));
        }
        self.providers.insert(id, provider);
        Ok(())
    }

    /// 按 ID 查找 Provider
    pub fn get(&self, id: &ProviderId) -> Option<Arc<dyn LLMProvider>> {
        self.providers.get(id).map(|e| e.clone())
    }

    /// 列举所有已注册的 Provider ID
    pub fn list(&self) -> Vec<ProviderId> {
        self.providers.iter().map(|e| e.key().clone()).collect()
    }

    /// 已注册 Provider 数量
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// 并行健康检查所有 Provider
    ///
    /// 每个 Provider 独立超时（`per_provider_timeout`），互不阻塞。
    /// 返回所有 Provider 的状态（含失败原因）。
    pub async fn health_check_all(
        &self,
        per_provider_timeout: Duration,
    ) -> Vec<ProviderStatus> {
        let entries: Vec<(ProviderId, Arc<dyn LLMProvider>)> = self
            .providers
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();

        let mut handles = Vec::with_capacity(entries.len());
        for (id, provider) in entries {
            handles.push(tokio::spawn(async move {
                let result =
                    tokio::time::timeout(per_provider_timeout, provider.health_check()).await;
                match result {
                    Ok(Ok(())) => ProviderStatus {
                        id,
                        healthy: true,
                        error: None,
                    },
                    Ok(Err(e)) => ProviderStatus {
                        id,
                        healthy: false,
                        error: Some(e.to_string()),
                    },
                    Err(_) => ProviderStatus {
                        id,
                        healthy: false,
                        error: Some("health check timeout".into()),
                    },
                }
            }));
        }

        let mut statuses = Vec::with_capacity(handles.len());
        for h in handles {
            match h.await {
                Ok(s) => statuses.push(s),
                Err(e) => tracing::warn!(error = %e, "health_check task panicked"),
            }
        }
        statuses
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{
        ChatRequest, ChatResponse, FinishReason, Message, ModelSpec, ProviderCapabilities,
        StreamChunk,
    };
    use futures::stream::BoxStream;

    struct StubProvider {
        id: ProviderId,
        healthy: bool,
    }

    #[async_trait::async_trait]
    impl LLMProvider for StubProvider {
        fn id(&self) -> ProviderId {
            self.id.clone()
        }
        fn capabilities(&self) -> &ProviderCapabilities {
            &STUB_CAPS
        }
        fn model_spec(&self) -> ModelSpec {
            ModelSpec {
                context_window_tokens: 4096,
                max_output_tokens: 1024,
            }
        }
        async fn chat(
            &self,
            _req: ChatRequest,
        ) -> Result<ChatResponse, LlmError> {
            if self.healthy {
                Ok(ChatResponse {
                    id: "stub".into(),
                    model: "stub".into(),
                    message: Message::user(""),
                    finish_reason: FinishReason::Stop,
                    usage: None,
                })
            } else {
                Err(LlmError::Server {
                    status: 503,
                    body: "unhealthy".into(),
                })
            }
        }
        async fn chat_stream(
            &self,
            _req: ChatRequest,
        ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
            Err(LlmError::BadRequest("stub does not stream".into()))
        }
    }

    static STUB_CAPS: ProviderCapabilities = ProviderCapabilities {
        parallel_tool_calls: false,
        system_role: true,
        streaming: false,
        usage_reported: false,
        multimodal: crate::provider::MultimodalCapabilities::NONE,
    };

    #[test]
    fn register_and_get() {
        let reg = ProviderRegistry::new();
        let p = Arc::new(StubProvider {
            id: ProviderId::new("test/m"),
            healthy: true,
        });
        reg.register(p).unwrap();
        assert_eq!(reg.len(), 1);
        assert!(reg.get(&ProviderId::new("test/m")).is_some());
        assert!(reg.get(&ProviderId::new("other/m")).is_none());
    }

    #[test]
    fn duplicate_register_fails() {
        let reg = ProviderRegistry::new();
        let p = Arc::new(StubProvider {
            id: ProviderId::new("test/m"),
            healthy: true,
        });
        reg.register(p.clone()).unwrap();
        let err = reg.register(p).unwrap_err();
        assert!(matches!(err, ProviderRegistryError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn health_check_all_mixed() {
        let reg = ProviderRegistry::new();
        reg.register(Arc::new(StubProvider {
            id: ProviderId::new("a/m"),
            healthy: true,
        }))
        .unwrap();
        reg.register(Arc::new(StubProvider {
            id: ProviderId::new("b/m"),
            healthy: false,
        }))
        .unwrap();

        let statuses = reg.health_check_all(Duration::from_secs(5)).await;
        assert_eq!(statuses.len(), 2);
        let healthy_count = statuses.iter().filter(|s| s.healthy).count();
        assert_eq!(healthy_count, 1);
    }
}
