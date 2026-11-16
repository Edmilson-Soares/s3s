//! Servidor S3 (s3s + hyper) rodando num runtime tokio próprio, com uma fila
//! de eventos (JSON) que o Fogu consome por `fs3_dispatch`.
//!
//! O callback do fogu:ffi só pode rodar na thread do interpretador, durante
//! uma chamada FFI, então as threads do tokio nunca chamam o Fogu: cada
//! requisição vira um evento `request` na fila e `fs3_dispatch` os entrega na
//! thread de quem chamou. As operações diretas (`store`) rodam com `block_on`
//! no mesmo runtime.

use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use http::HeaderValue;
use http::header::{AUTHORIZATION, CONTENT_LENGTH, TRANSFER_ENCODING, USER_AGENT};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use hyper_util::server::graceful::GracefulShutdown;
use s3s::auth::SimpleAuth;
use s3s::host::SingleDomain;
use s3s::service::{S3Service, S3ServiceBuilder};
use s3s::{Body, HttpError, HttpResponse};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::{oneshot, watch};

use crate::store::Store;

/// Opções de `fs3_listen`, lidas do JSON.
pub struct Options {
    pub bind: String,
    pub root: PathBuf,
    pub credentials: Vec<(String, String)>,
    pub domain: Option<String>,
    pub events: bool,
}

impl Options {
    pub fn from_json(text: &str) -> Result<Options, String> {
        let v: Value = if text.trim().is_empty() {
            json!({})
        } else {
            serde_json::from_str(text).map_err(|e| format!("invalid config JSON: {e}"))?
        };
        let str_of = |key: &str| v.get(key).and_then(Value::as_str).map(str::to_string);

        let mut credentials = Vec::new();
        match (str_of("access_key"), str_of("secret_key")) {
            (Some(a), Some(s)) => credentials.push((a, s)),
            (None, None) => {}
            _ => return Err("access_key and secret_key must be given together".into()),
        }
        if let Some(map) = v.get("credentials") {
            let map = map.as_object().ok_or("credentials must be a map { access_key: secret_key }")?;
            for (a, s) in map {
                let s = s.as_str().ok_or_else(|| format!("credentials.{a}: secret must be a string"))?;
                credentials.push((a.clone(), s.to_string()));
            }
        }

        Ok(Options {
            bind: str_of("bind").unwrap_or_else(|| "127.0.0.1:9000".into()),
            root: PathBuf::from(str_of("root").unwrap_or_else(|| "data".into())),
            credentials,
            domain: str_of("domain").filter(|d| !d.is_empty()),
            events: v.get("events").and_then(Value::as_bool).unwrap_or(true),
        })
    }
}

/// Um evento da fila (só JSON; não há payload binário neste servidor).
pub struct Event {
    pub json: String,
}

struct Shared {
    events: mpsc::Sender<Event>,
    emit_requests: bool,
    requests: AtomicU64,
}

impl Shared {
    fn emit(&self, event: Value) {
        let _ = self.events.send(Event { json: event.to_string() });
    }
}

struct ServerState {
    addr: SocketAddr,
    auth: bool,
    shutdown: watch::Sender<bool>,
    done: Mutex<Option<oneshot::Receiver<()>>>,
}

/// Um armazenamento aberto (`fs3_open`) ou um servidor (`fs3_listen`): os dois
/// têm o `Store` e o runtime; só o segundo tem o `ServerState`.
pub struct Instance {
    rt: Runtime,
    pub store: Store,
    root: PathBuf,
    shared: Arc<Shared>,
    events: Mutex<mpsc::Receiver<Event>>,
    server: Option<ServerState>,
}

fn display_path(p: &Path) -> String {
    let s = p.to_string_lossy();
    s.strip_prefix(r"\\?\").unwrap_or(&s).to_string()
}

impl Instance {
    fn new(root: &Path, emit_requests: bool) -> Result<Instance, String> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("fogus3s")
            .enable_all()
            .build()
            .map_err(|e| format!("failed to start runtime: {e}"))?;
        let store = Store::open(root)?;
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
        let (tx, rx) = mpsc::channel();
        Ok(Instance {
            rt,
            store,
            root,
            shared: Arc::new(Shared { events: tx, emit_requests, requests: AtomicU64::new(0) }),
            events: Mutex::new(rx),
            server: None,
        })
    }

    /// Só o armazenamento, sem servidor.
    pub fn open(root: &Path) -> Result<Instance, String> {
        Instance::new(root, false)
    }

    /// Armazenamento + servidor HTTP escutando em `opts.bind`.
    pub fn listen(opts: Options) -> Result<Instance, String> {
        let mut inst = Instance::new(&opts.root, opts.events)?;

        let service = {
            let mut b = S3ServiceBuilder::new(inst.store.clone());
            if !opts.credentials.is_empty() {
                let mut auth = SimpleAuth::new();
                for (a, s) in &opts.credentials {
                    auth.register(a.clone(), s.as_str().into());
                }
                b.set_auth(auth);
            }
            if let Some(domain) = &opts.domain {
                b.set_host(SingleDomain::new(domain).map_err(|e| format!("invalid domain '{domain}': {e}"))?);
            }
            b.build()
        };

        let listener = inst
            .rt
            .block_on(TcpListener::bind(&opts.bind))
            .map_err(|e| format!("failed to bind {}: {e}", opts.bind))?;
        let addr = listener.local_addr().map_err(|e| e.to_string())?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (done_tx, done_rx) = oneshot::channel();
        inst.rt.spawn(accept_loop(listener, service, inst.shared.clone(), shutdown_rx, done_tx));

        let auth = !opts.credentials.is_empty();
        inst.shared.emit(json!({
            "type": "listening",
            "address": addr.ip().to_string(),
            "port": addr.port(),
            "root": display_path(&inst.root),
            "auth": auth,
        }));
        inst.server = Some(ServerState { addr, auth, shutdown: shutdown_tx, done: Mutex::new(Some(done_rx)) });
        Ok(inst)
    }

    /// `{ address, port, url, root, auth, listening, requests }`
    pub fn info(&self) -> Value {
        let mut v = json!({
            "root": display_path(&self.root),
            "listening": self.server.is_some(),
            "requests": self.shared.requests.load(Ordering::Relaxed),
            "auth": false,
            "address": Value::Null,
            "port": Value::Null,
            "url": Value::Null,
        });
        if let Some(s) = &self.server {
            v["address"] = s.addr.ip().to_string().into();
            v["port"] = s.addr.port().into();
            v["url"] = format!("http://{}", s.addr).into();
            v["auth"] = s.auth.into();
        }
        v
    }

    pub fn requests(&self) -> u64 {
        self.shared.requests.load(Ordering::Relaxed)
    }

    pub fn block_on<F: Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }

    /// Espera até `timeout` pelo primeiro evento e entrega a `deliver` até `max`
    /// eventos de uma vez (os seguintes sem esperar). `deliver` devolve false para
    /// parar. Retorna quantos foram entregues.
    pub fn drain(&self, timeout: Duration, max: usize, mut deliver: impl FnMut(&Event) -> bool) -> usize {
        let rx = self.events.lock().unwrap_or_else(|e| e.into_inner());
        let mut n = 0;
        let mut wait = timeout;
        while n < max.max(1) {
            let ev = if wait.is_zero() { rx.try_recv().ok() } else { rx.recv_timeout(wait).ok() };
            let Some(ev) = ev else { break };
            n += 1;
            wait = Duration::ZERO;
            if !deliver(&ev) {
                break;
            }
        }
        n
    }

    /// Para o servidor (se houver), espera as conexões fecharem e encerra o runtime.
    pub fn stop(self) {
        if let Some(s) = &self.server {
            let _ = s.shutdown.send(true);
            if let Some(done) = s.done.lock().unwrap_or_else(|e| e.into_inner()).take() {
                let _ = self.rt.block_on(async { tokio::time::timeout(Duration::from_secs(3), done).await });
            }
        }
        self.rt.shutdown_timeout(Duration::from_secs(2));
    }
}

async fn accept_loop(
    listener: TcpListener,
    service: S3Service,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
    done: oneshot::Sender<()>,
) {
    let http = ConnBuilder::new(TokioExecutor::new());
    let graceful = GracefulShutdown::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((socket, peer)) => {
                    let svc = Logged { inner: service.clone(), shared: shared.clone(), peer };
                    let conn = graceful.watch(http.serve_connection(TokioIo::new(socket), svc).into_owned());
                    tokio::spawn(async move {
                        let _ = conn.await;
                    });
                }
                Err(e) => {
                    shared.emit(json!({ "type": "error", "message": format!("accept: {e}") }));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            _ = shutdown.changed() => break,
        }
    }
    drop(listener);
    let _ = tokio::time::timeout(Duration::from_secs(2), graceful.shutdown()).await;
    let _ = done.send(());
}

/// `/bucket/key...` (path-style) -> (bucket, key). Com domínio virtual o
/// bucket vem do host e não aparece aqui.
fn split_path(path: &str) -> (Option<&str>, Option<&str>) {
    let rest = path.trim_start_matches('/');
    if rest.is_empty() {
        return (None, None);
    }
    match rest.split_once('/') {
        Some((b, k)) => (Some(b), if k.is_empty() { None } else { Some(k) }),
        None => (Some(rest), None),
    }
}

/// `SignedHeaders=...` do cabeçalho Authorization (SigV4) inclui `name`?
fn signed_header_listed(authorization: &str, name: &str) -> bool {
    authorization
        .split(',')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("SignedHeaders="))
        .is_some_and(|list| list.split(';').any(|h| h.eq_ignore_ascii_case(name)))
}

/// Compatibilidade com clientes que assinam `content-length` em pedidos sem
/// corpo (o `fogu:s3` assina `content-length: 0` num GET, mas o hyper do lado
/// dele não envia o cabeçalho): a s3s recusaria com "missing signed header".
/// Repõe `content-length: 0` quando ele foi assinado e o pedido não tem corpo
/// declarado, sem mudar o que a s3s verifica nos demais casos.
fn restore_signed_content_length(req: &mut http::Request<Incoming>) {
    let headers = req.headers();
    if headers.contains_key(CONTENT_LENGTH) || headers.contains_key(TRANSFER_ENCODING) {
        return;
    }
    let signed = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| signed_header_listed(a, "content-length"));
    if signed {
        req.headers_mut().insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_headers_and_paths() {
        let auth = "AWS4-HMAC-SHA256 Credential=ak/20261110/us-east-1/s3/aws4_request, SignedHeaders=content-length;host;x-amz-date, Signature=abc";
        assert!(signed_header_listed(auth, "content-length"));
        assert!(signed_header_listed(auth, "Host"));
        assert!(!signed_header_listed(auth, "content-type"));
        assert!(!signed_header_listed("AWS4-HMAC-SHA256 Credential=x, Signature=y", "host"));

        assert_eq!(split_path("/"), (None, None));
        assert_eq!(split_path("/docs"), (Some("docs"), None));
        assert_eq!(split_path("/docs/"), (Some("docs"), None));
        assert_eq!(split_path("/docs/a/b.txt"), (Some("docs"), Some("a/b.txt")));
    }

    #[test]
    fn options_from_json() {
        let o = Options::from_json("").unwrap();
        assert_eq!(o.bind, "127.0.0.1:9000");
        assert!(o.credentials.is_empty() && o.events);
        let o = Options::from_json(r#"{"access_key": "a", "secret_key": "s", "credentials": {"b": "t"}, "events": false}"#).unwrap();
        assert_eq!(o.credentials, vec![("a".into(), "s".into()), ("b".into(), "t".into())]);
        assert!(!o.events);
        assert!(Options::from_json(r#"{"access_key": "a"}"#).is_err());
    }
}

/// Envolve o `S3Service`: conta as requisições e emite o evento `request`
/// (método, caminho, bucket/chave, status, duração, cliente) de cada uma.
#[derive(Clone)]
struct Logged {
    inner: S3Service,
    shared: Arc<Shared>,
    peer: SocketAddr,
}

impl hyper::service::Service<http::Request<Incoming>> for Logged {
    type Response = HttpResponse;
    type Error = HttpError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, mut req: http::Request<Incoming>) -> Self::Future {
        restore_signed_content_length(&mut req);
        let inner = self.inner.clone();
        let shared = self.shared.clone();
        let peer = self.peer;
        let method = req.method().to_string();
        let path = req.uri().path().to_string();
        let query = req.uri().query().map(str::to_string);
        let header = |name| req.headers().get(name).and_then(|v| v.to_str().ok()).map(str::to_string);
        let agent = header(USER_AGENT);
        let bytes_in = header(CONTENT_LENGTH).and_then(|v| v.parse::<u64>().ok());
        Box::pin(async move {
            let t0 = Instant::now();
            let res = inner.call(req.map(Body::from)).await;
            shared.requests.fetch_add(1, Ordering::Relaxed);
            if shared.emit_requests {
                let (status, bytes_out, error) = match &res {
                    Ok(r) => (
                        Value::from(r.status().as_u16()),
                        http_body::Body::size_hint(r.body()).exact().map(Value::from).unwrap_or(Value::Null),
                        Value::Null,
                    ),
                    Err(e) => (Value::Null, Value::Null, Value::from(e.to_string())),
                };
                let (bucket, key) = split_path(&path);
                shared.emit(json!({
                    "type": "request",
                    "method": method,
                    "path": path,
                    "query": query,
                    "bucket": bucket,
                    "key": key,
                    "status": status,
                    "ms": t0.elapsed().as_secs_f64() * 1000.0,
                    "bytes_in": bytes_in,
                    "bytes_out": bytes_out,
                    "remote": peer.to_string(),
                    "user_agent": agent,
                    "error": error,
                }));
            }
            res
        })
    }
}
