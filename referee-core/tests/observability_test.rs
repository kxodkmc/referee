//! Phase 5 测试：可观测层
//! Tracing 全链路关联（trace_id 穿透）/ Metrics 核心指标（队列深度 / 处理延迟 / Panic / 路由失败）
//!
//! 依赖说明：`metrics` 0.22 无内置 testing recorder（0.23+ 才引入），
//! 本文件自带轻量内存 Recorder（实现 `metrics::Recorder`，仅测试用，不进入内核）。

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use referee_core::kernel::priority::PrioritySender;
use referee_core::{
    CapabilityId, Envelope, Extension, Kernel, KernelContext, KernelError, KernelResult,
    MessageContext, SupervisionPolicy,
};
use tracing_subscriber::fmt::format::FmtSpan;

/// 共享输出 writer：tracing fmt 的 MakeWriter，写回 Arc 共享 buffer 供断言
#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl SharedWriter {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn buffer(&self) -> Arc<Mutex<Vec<u8>>> {
        self.0.clone()
    }
}

impl io::Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
    type Writer = SharedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

// ───────────────────────────────────────────────
// 测试专用内存 Recorder（metrics 0.22 门面实现）
// ───────────────────────────────────────────────

#[derive(Default)]
struct TestData {
    counters: Mutex<Vec<CounterEntry>>,
    gauges: Mutex<Vec<GaugeEntry>>,
    histograms: Mutex<Vec<HistogramEntry>>,
}

struct CounterEntry {
    name: String,
    labels: Vec<(String, String)>,
    handle: Arc<AtomicU64>,
}

struct GaugeEntry {
    name: String,
    labels: Vec<(String, String)>,
    handle: Arc<Mutex<f64>>,
}

struct HistogramEntry {
    name: String,
    labels: Vec<(String, String)>,
    handle: Arc<Mutex<Vec<f64>>>,
}

/// 句柄包装：metrics 句柄类型要求自定义类型实现对应 Fn trait
struct TestCounter(Arc<AtomicU64>);
struct TestGauge(Arc<Mutex<f64>>);
struct TestHistogram(Arc<Mutex<Vec<f64>>>);

impl CounterFn for TestCounter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::SeqCst);
    }
    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::SeqCst);
    }
}

impl GaugeFn for TestGauge {
    fn increment(&self, value: f64) {
        *self.0.lock().unwrap() += value;
    }
    fn decrement(&self, value: f64) {
        *self.0.lock().unwrap() -= value;
    }
    fn set(&self, value: f64) {
        *self.0.lock().unwrap() = value;
    }
}

impl HistogramFn for TestHistogram {
    fn record(&self, value: f64) {
        self.0.lock().unwrap().push(value);
    }
}

struct TestRecorder {
    inner: Arc<TestData>,
}

fn key_parts(key: &Key) -> (String, Vec<(String, String)>) {
    let labels = key
        .labels()
        .map(|l| (l.key().to_string(), l.value().to_string()))
        .collect();
    (key.name().to_string(), labels)
}

impl Recorder for TestRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    /// 注册即去重：同一 (name, labels) 复用句柄，保证多次 set/record 落到同一存储
    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let (name, labels) = key_parts(key);
        let mut entries = self.inner.counters.lock().unwrap();
        if let Some(e) = entries
            .iter()
            .find(|e| e.name == name && e.labels == labels)
        {
            return Counter::from_arc(Arc::new(TestCounter(e.handle.clone())));
        }
        let handle = Arc::new(AtomicU64::new(0));
        entries.push(CounterEntry {
            name,
            labels,
            handle: handle.clone(),
        });
        Counter::from_arc(Arc::new(TestCounter(handle)))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        let (name, labels) = key_parts(key);
        let mut entries = self.inner.gauges.lock().unwrap();
        if let Some(e) = entries
            .iter()
            .find(|e| e.name == name && e.labels == labels)
        {
            return Gauge::from_arc(Arc::new(TestGauge(e.handle.clone())));
        }
        let handle = Arc::new(Mutex::new(0.0));
        entries.push(GaugeEntry {
            name,
            labels,
            handle: handle.clone(),
        });
        Gauge::from_arc(Arc::new(TestGauge(handle)))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        let (name, labels) = key_parts(key);
        let mut entries = self.inner.histograms.lock().unwrap();
        if let Some(e) = entries
            .iter()
            .find(|e| e.name == name && e.labels == labels)
        {
            return Histogram::from_arc(Arc::new(TestHistogram(e.handle.clone())));
        }
        let handle = Arc::new(Mutex::new(Vec::new()));
        entries.push(HistogramEntry {
            name,
            labels,
            handle: handle.clone(),
        });
        Histogram::from_arc(Arc::new(TestHistogram(handle)))
    }
}

fn labels_match(have: &[(String, String)], want: &[(&str, &str)]) -> bool {
    want.iter()
        .all(|(k, v)| have.iter().any(|(hk, hv)| hk == k && hv == v))
}

impl TestData {
    fn counter(&self, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
        self.counters
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.name == name && labels_match(&e.labels, labels))
            .map(|e| e.handle.load(Ordering::SeqCst))
    }

    fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        self.gauges
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.name == name && labels_match(&e.labels, labels))
            .map(|e| *e.handle.lock().unwrap())
    }

    fn histogram_samples(&self, name: &str, labels: &[(&str, &str)]) -> Option<Vec<f64>> {
        self.histograms
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.name == name && labels_match(&e.labels, labels))
            .map(|e| e.handle.lock().unwrap().clone())
    }
}

// ───────────────────────────────────────────────
// 全局初始化样板（Once 防多测试并行重复初始化）
// ───────────────────────────────────────────────

static INIT: Once = Once::new();
static DATA: OnceLock<Arc<TestData>> = OnceLock::new();
static OUTPUT: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();

fn init_observability() {
    INIT.call_once(|| {
        // metrics：全局内存 recorder（仅测试进程内生效）
        let data = Arc::new(TestData::default());
        let recorder = TestRecorder {
            inner: data.clone(),
        };
        let _ = metrics::set_global_recorder(recorder);
        let _ = DATA.set(data);

        // tracing：fmt 输出重定向到共享 buffer，并输出 span 创建事件
        //（kernel_dispatch / extension_handle 的 trace_id 需要 span 事件行承载）
        let writer = SharedWriter::new();
        let output = writer.buffer();
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_target(false)
            .with_ansi(false)
            .with_span_events(FmtSpan::NEW)
            .with_writer(writer)
            .try_init();
        let _ = OUTPUT.set(output);
    });
}

fn data() -> &'static Arc<TestData> {
    DATA.get()
        .expect("init_observability() must be called first")
}

fn output_text() -> String {
    let output = OUTPUT
        .get()
        .expect("init_observability() must be called first");
    String::from_utf8_lossy(&output.lock().unwrap()).into_owned()
}

/// 从 fmt 输出行提取 trace_id（uuid v4 格式：字母数字 + 连字符）
fn trace_id_of(line: &str) -> Option<String> {
    const MARKER: &str = "trace_id=";
    let start = line.find(MARKER)? + MARKER.len();
    let rest = &line[start..];
    let end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '-'))
        .unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

// ───────────────────────────────────────────────
// 测试夹具
// ───────────────────────────────────────────────

/// 可控延迟扩展（验证处理延迟直方图 / 路由背压）
struct SlowExtension {
    id: CapabilityId,
    ms: u64,
}

#[async_trait]
impl Extension for SlowExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        tokio::time::sleep(Duration::from_millis(self.ms)).await;
        Ok(())
    }
}

/// 主动 Panic 扩展（验证 outcome=panic 直方图 + Panic 计数器）
struct PanicExtension {
    id: CapabilityId,
}

#[async_trait]
impl Extension for PanicExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        panic!("simulated panic for observability");
    }
}

/// 在 handle 内输出日志的扩展（验证 trace_id 穿透关联）
struct SpanExtension {
    id: CapabilityId,
}

#[async_trait]
impl Extension for SpanExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        tracing::info!(trace_id = %env.trace_id, "span handled");
        let mut resp = Envelope::new();
        resp.correlation_id = env.correlation_id;
        ctx.reply(resp)
    }
}

// ───────────────────────────────────────────────
// 用例 1：Span 穿透与关联 — kernel_dispatch 与 extension_handle 共享同一 trace_id
// ───────────────────────────────────────────────
#[tokio::test]
async fn trace_id_correlates_dispatch_and_handle() {
    init_observability();
    let kernel = Kernel::new();
    let ext = SpanExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    let req = Envelope::new();
    let expected_trace = req.trace_id.to_string();
    let _ = kernel.invoke(ext_id, req, 1000).await;
    // 等待 fmt 输出落盘（扩展处理与 span 事件均为同步写入，此处仅作保险）
    tokio::time::sleep(Duration::from_millis(50)).await;

    let text = output_text();
    // 用本请求的 trace_id 过滤，避免并行测试的 span 事件混入干扰断言
    let dispatch_trace = text
        .lines()
        .find(|l| l.contains("kernel_dispatch") && l.contains(&expected_trace))
        .and_then(trace_id_of);
    let handle_trace = text
        .lines()
        .find(|l| l.contains("extension_handle") && l.contains(&expected_trace))
        .and_then(trace_id_of);

    assert!(
        dispatch_trace.is_some(),
        "kernel_dispatch span event missing (is tracing subscriber initialized?):\n{text}"
    );
    assert!(
        handle_trace.is_some(),
        "extension_handle span event missing:\n{text}"
    );
    assert_eq!(
        dispatch_trace, handle_trace,
        "trace_id must propagate from dispatch to handle:\n{text}"
    );
}

// ───────────────────────────────────────────────
// 用例 2：队列深度指标 — 入队 5 条深度为 5，全部消费后回落至 0
// ───────────────────────────────────────────────
#[tokio::test]
async fn queue_depth_gauge_tracks_inflight() {
    init_observability();
    let ext_id = CapabilityId::new();
    let (tx, rx) = PrioritySender::new(5, ext_id);
    let label = ext_id.to_string();

    for _ in 0..5 {
        tx.try_send(MessageContext::new(Envelope::new()))
            .expect("send ok");
    }
    assert_eq!(
        data().gauge("referee_queue_depth", &[("ext_id", &label)]),
        Some(5.0),
        "depth must equal number of queued messages"
    );

    for _ in 0..5 {
        rx.recv().await.expect("recv ok");
    }
    assert_eq!(
        data().gauge("referee_queue_depth", &[("ext_id", &label)]),
        Some(0.0),
        "depth must fall back to 0 after consumption"
    );
}

// ───────────────────────────────────────────────
// 用例 3：处理延迟直方图 + Panic 指标
// ───────────────────────────────────────────────
#[tokio::test]
async fn handle_duration_and_panic_metrics() {
    init_observability();
    let kernel = Kernel::new();

    // ok 样本：sleep 100ms
    let slow = SlowExtension {
        id: CapabilityId::new(),
        ms: 100,
    };
    let slow_id = slow.id();
    kernel
        .register(Box::new(slow), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    kernel
        .emit(slow_id, Envelope::new())
        .await
        .expect("emit ok");
    tokio::time::sleep(Duration::from_millis(250)).await;

    let ok_samples = data()
        .histogram_samples(
            "referee_handle_duration_seconds",
            &[("ext_id", &slow_id.to_string()), ("outcome", "ok")],
        )
        .expect("ok histogram must be recorded");
    assert!(!ok_samples.is_empty(), "ok sample missing");
    // 下限：tokio sleep 保证至少 100ms；上限放宽以容忍并行 runtime 的 CPU 争用
    assert!(
        (0.05..0.35).contains(&ok_samples[0]),
        "ok duration {:?} should be ~0.1s",
        ok_samples[0]
    );

    // panic 样本：主动 panic
    let panicky = PanicExtension {
        id: CapabilityId::new(),
    };
    let panic_id = panicky.id();
    kernel
        .register(Box::new(panicky), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    let _ = kernel.emit(panic_id, Envelope::new()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let panic_samples = data()
        .histogram_samples(
            "referee_handle_duration_seconds",
            &[("ext_id", &panic_id.to_string()), ("outcome", "panic")],
        )
        .expect("panic histogram must be recorded");
    assert!(!panic_samples.is_empty(), "panic sample missing");
    assert_eq!(
        data().counter(
            "referee_extension_panics_total",
            &[("ext_id", &panic_id.to_string())]
        ),
        Some(1),
        "panic counter must increment exactly once"
    );
}

// ───────────────────────────────────────────────
// 用例 4：路由失败指标 — 队列满时 referee_dispatch_total{result=full} 递增
// ───────────────────────────────────────────────
#[tokio::test]
async fn dispatch_full_counter_increments() {
    init_observability();
    let kernel = Kernel::new();
    // 慢消费扩展（200ms/条）+ 单容量队列：3 条 emit 必有一条命中 full
    let slow = SlowExtension {
        id: CapabilityId::new(),
        ms: 200,
    };
    let ext_id = slow.id();
    kernel
        .register(Box::new(slow), 1, SupervisionPolicy::Transient)
        .await
        .unwrap();
    // 等待运行循环启动（提高"第 1 条已被消费、第 3 条必满"的确定性）
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut saw_full = false;
    for _ in 0..3 {
        match kernel.emit(ext_id, Envelope::new()).await {
            Ok(()) => {}
            Err(KernelError::ResourceExhausted) => saw_full = true,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert!(saw_full, "queue of capacity 1 must eventually reject");

    let full = data()
        .counter(
            "referee_dispatch_total",
            &[("ext_id", &ext_id.to_string()), ("result", "full")],
        )
        .expect("dispatch counter with result=full must be recorded");
    assert!(
        full >= 1,
        "result=full counter should increment, got {full}"
    );
}
