//! fogus3s — servidor S3 (crate `s3s` + backend em disco `s3s-fs`) com API C
//! para o `fogu:ffi`, e acesso direto aos buckets/objetos sem passar por HTTP.
//!
//! Convenções (Fogu 0.7+: o fogu:ffi aceita qualquer número de argumentos e
//! callbacks; ver `cb.rs` e `fs3_set_sink`):
//! - Armazenamentos e servidores são handles `i64` (> 0): `fs3_open` abre só
//!   a pasta, `fs3_listen` abre a pasta e sobe o servidor; `fs3_close` fecha os dois.
//! - Funções de status retornam `0` em sucesso e `-1` em erro; a mensagem
//!   fica em `fs3_last_error`. Os erros do S3 vêm como `Codigo: mensagem`
//!   (`NoSuchKey: The specified key does not exist.`).
//! - Saídas (JSON ou bytes) vão para o buffer do chamador (`ffi.alloc`),
//!   JSON terminado em NUL; retorno = tamanho escrito, `-(tamanho necessário)`
//!   se o buffer for pequeno, ou -1 em erro. Com um sink registrado
//!   (`fs3_set_sink`), `buf = NULL, cap = 0` entrega a saída inteira pelo callback.
//! - Eventos: `fs3_dispatch` entrega um lote pelo callback `EventFn`
//!   (`listening`, `request`, `error`); não há payload binário.

mod cb;
mod server;
mod store;

use std::collections::HashMap;
use std::ffi::{CStr, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};

use server::{Instance, Options};

static INSTANCES: LazyLock<Mutex<HashMap<i64, Arc<Instance>>>> = LazyLock::new(Default::default);
static NEXT_HANDLE: AtomicI64 = AtomicI64::new(1);
static LAST_ERROR: Mutex<String> = Mutex::new(String::new());

fn set_error(msg: impl Into<String>) {
    *LAST_ERROR.lock().unwrap() = msg.into();
}

/// Executa `f` sem deixar um panic atravessar a fronteira C.
fn guard<T>(on_error: T, f: impl FnOnce() -> Result<T, String>) -> T {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            set_error(e);
            on_error
        }
        Err(_) => {
            set_error("internal panic in fogus3s");
            on_error
        }
    }
}

fn status(f: impl FnOnce() -> Result<(), String>) -> i32 {
    guard(-1, || f().map(|_| 0))
}

fn flag(f: impl FnOnce() -> Result<bool, String>) -> i32 {
    guard(-1, || f().map(i32::from))
}

fn get(handle: i64) -> Result<Arc<Instance>, String> {
    INSTANCES.lock().unwrap().get(&handle).cloned().ok_or_else(|| format!("invalid handle {handle}"))
}

fn register(inst: Instance) -> i64 {
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
    INSTANCES.lock().unwrap().insert(handle, Arc::new(inst));
    handle
}

unsafe fn text<'a>(ptr: *const c_char) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Ok("");
    }
    unsafe { CStr::from_ptr(ptr) }.to_str().map_err(|_| "string is not valid UTF-8".to_string())
}

unsafe fn json_arg(ptr: *const c_char) -> Result<Value, String> {
    let s = unsafe { text(ptr) }?;
    if s.trim().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str(s).map_err(|e| format!("invalid JSON argument: {e}"))
}

unsafe fn bytes_arg(data: *const u8, len: i64) -> Result<Bytes, String> {
    if len < 0 || (data.is_null() && len > 0) {
        return Err("invalid buffer".into());
    }
    if len == 0 {
        return Ok(Bytes::new());
    }
    Ok(Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, len as usize) }))
}

/// Bytes crus: pelo sink (buf = NULL, cap = 0) ou no buffer do chamador (sem NUL).
unsafe fn write_bytes(buf: *mut u8, cap: i64, data: &[u8]) -> i64 {
    if let Some(n) = cb::try_sink(buf, cap, data) {
        return n;
    }
    if buf.is_null() || cap < data.len() as i64 {
        return -(data.len() as i64);
    }
    unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf, data.len()) };
    data.len() as i64
}

/// Texto: pelo sink ou no buffer do chamador, terminado em NUL.
unsafe fn write_out(buf: *mut u8, cap: i64, s: &str) -> i64 {
    if let Some(n) = cb::try_sink(buf, cap, s.as_bytes()) {
        return n;
    }
    let need = s.len() + 1;
    if buf.is_null() || cap < need as i64 {
        return -(need as i64);
    }
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr(), buf, s.len());
        *buf.add(s.len()) = 0;
    }
    s.len() as i64
}

unsafe fn write_json(buf: *mut u8, cap: i64, value: Value) -> i64 {
    unsafe { write_out(buf, cap, &value.to_string()) }
}

/// Versão da biblioteca. Retorna o tamanho escrito (ver convenções).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_version(buf: *mut u8, cap: i64) -> i64 {
    unsafe { write_out(buf, cap, env!("CARGO_PKG_VERSION")) }
}

/// Última mensagem de erro. Retorna o tamanho escrito (0 se não há erro).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_last_error(buf: *mut u8, cap: i64) -> i64 {
    let msg = LAST_ERROR.lock().unwrap().clone();
    unsafe { write_out(buf, cap, &msg) }
}

/// Registra (ou remove, com NULL) o callback (Fogu 0.7+, `ffi.callback`) que
/// recebe as saídas das funções chamadas com `buf = NULL, cap = 0` (ver `cb.rs`).
#[unsafe(no_mangle)]
pub extern "C" fn fs3_set_sink(sink: Option<cb::Sink>) -> i32 {
    cb::set_sink(sink);
    0
}

// ---- armazenamento e servidor ----------------------------------------

/// Abre (criando se preciso) a pasta `root` como armazenamento S3, sem
/// servidor: só as operações diretas. Retorna o handle (> 0) ou -1.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_open(root: *const c_char) -> i64 {
    guard(-1, || {
        let root = unsafe { text(root) }?;
        let root = if root.is_empty() { "data" } else { root };
        Ok(register(Instance::open(Path::new(root))?))
    })
}

/// Abre o armazenamento e sobe o servidor S3. `config_json`: `{"bind":
/// "127.0.0.1:9000", "root": "data", "access_key", "secret_key",
/// "credentials": {ak: sk, ...}, "domain", "events": true}`. Sem credenciais
/// o servidor aceita requisições anônimas. Retorna o handle (> 0) ou -1.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_listen(config_json: *const c_char) -> i64 {
    guard(-1, || {
        let opts = Options::from_json(unsafe { text(config_json) }?)?;
        Ok(register(Instance::listen(opts)?))
    })
}

/// `{"address", "port", "url", "root", "auth", "listening", "requests"}`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_info(handle: i64, buf: *mut u8, cap: i64) -> i64 {
    guard(-1, || Ok(unsafe { write_json(buf, cap, get(handle)?.info()) }))
}

/// Requisições HTTP atendidas até agora, ou -1.
#[unsafe(no_mangle)]
pub extern "C" fn fs3_requests(handle: i64) -> i64 {
    guard(-1, || Ok(get(handle)?.requests() as i64))
}

/// Espera até `timeout_ms` pelo primeiro evento e entrega até `max` eventos
/// (os já enfileirados, sem esperar de novo) chamando `on_event(json, len, bin, len)`
/// na thread de quem chamou. Retorna quantos foram entregues (0 = nada no
/// tempo) ou -1 em erro.
#[unsafe(no_mangle)]
pub extern "C" fn fs3_dispatch(handle: i64, timeout_ms: i64, max: i64, on_event: Option<cb::EventFn>) -> i64 {
    guard(-1, || {
        let f = on_event.ok_or("dispatch: on_event is NULL")?;
        let inst = get(handle)?;
        let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
        let n = inst.drain(timeout, max.max(1) as usize, |ev| cb::event(f, &ev.json, &[]));
        Ok(n as i64)
    })
}

/// Para o servidor (se houver), fecha o armazenamento e libera o handle.
#[unsafe(no_mangle)]
pub extern "C" fn fs3_close(handle: i64) -> i32 {
    status(|| {
        let mut map = INSTANCES.lock().unwrap();
        let inst = map.remove(&handle).ok_or_else(|| format!("invalid handle {handle}"))?;
        match Arc::try_unwrap(inst) {
            Ok(inst) => {
                drop(map);
                inst.stop();
                Ok(())
            }
            Err(inst) => {
                map.insert(handle, inst);
                Err("instance is busy in another call".into())
            }
        }
    })
}

// ---- buckets ------------------------------------------------------------

/// `[{"name", "created"}]`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_buckets(handle: i64, buf: *mut u8, cap: i64) -> i64 {
    guard(-1, || {
        let inst = get(handle)?;
        let v = inst.block_on(inst.store.buckets())?;
        Ok(unsafe { write_json(buf, cap, v) })
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_bucket_create(handle: i64, name: *const c_char) -> i32 {
    status(|| {
        let name = unsafe { text(name) }?;
        let inst = get(handle)?;
        inst.block_on(inst.store.bucket_create(name))
    })
}

/// Apaga o bucket. Com `force = 0` precisa estar vazio (`BucketNotEmpty`);
/// com `force != 0` apaga também os objetos.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_bucket_delete(handle: i64, name: *const c_char, force: i32) -> i32 {
    status(|| {
        let name = unsafe { text(name) }?;
        let inst = get(handle)?;
        inst.block_on(inst.store.bucket_delete(name, force != 0))
    })
}

/// 1 se existe, 0 se não, -1 em erro.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_bucket_exists(handle: i64, name: *const c_char) -> i32 {
    flag(|| {
        let name = unsafe { text(name) }?;
        let inst = get(handle)?;
        inst.block_on(inst.store.bucket_exists(name))
    })
}

// ---- objetos ------------------------------------------------------------

/// Lista objetos (ListObjectsV2). `prefix`, `delimiter` e `token` podem ser
/// vazios; `max` <= 0 usa o padrão (1000). Escreve `{"objects": [{"key",
/// "size", "etag", "modified"}], "prefixes": [..], "truncated", "next"}`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_list(
    handle: i64,
    bucket: *const c_char,
    prefix: *const c_char,
    delimiter: *const c_char,
    max: i64,
    token: *const c_char,
    buf: *mut u8,
    cap: i64,
) -> i64 {
    guard(-1, || {
        let (bucket, prefix, delimiter, token) =
            unsafe { (text(bucket)?, text(prefix)?, text(delimiter)?, text(token)?) };
        let inst = get(handle)?;
        let v = inst.block_on(inst.store.list(bucket, prefix, delimiter, max, token))?;
        Ok(unsafe { write_json(buf, cap, v) })
    })
}

/// Grava `len` bytes de `data` em `bucket/key`. `opts_json`: `{"content_type",
/// "cache_control", "content_encoding", "content_disposition", "metadata": {..}}`.
/// Escreve `{"etag", "size"}`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_put(
    handle: i64,
    bucket: *const c_char,
    key: *const c_char,
    data: *const u8,
    len: i64,
    opts_json: *const c_char,
    buf: *mut u8,
    cap: i64,
) -> i64 {
    guard(-1, || {
        let (bucket, key) = unsafe { (text(bucket)?, text(key)?) };
        let data = unsafe { bytes_arg(data, len) }?;
        let opts = unsafe { json_arg(opts_json) }?;
        let inst = get(handle)?;
        let v = inst.block_on(inst.store.put(bucket, key, data, &opts))?;
        Ok(unsafe { write_json(buf, cap, v) })
    })
}

/// Como `fs3_put`, mas envia o arquivo `path` em stream.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_put_file(
    handle: i64,
    bucket: *const c_char,
    key: *const c_char,
    path: *const c_char,
    opts_json: *const c_char,
    buf: *mut u8,
    cap: i64,
) -> i64 {
    guard(-1, || {
        let (bucket, key, path) = unsafe { (text(bucket)?, text(key)?, text(path)?) };
        let opts = unsafe { json_arg(opts_json) }?;
        let inst = get(handle)?;
        let v = inst.block_on(inst.store.put_file(bucket, key, path, &opts))?;
        Ok(unsafe { write_json(buf, cap, v) })
    })
}

/// Lê o objeto (ou a faixa `range`, como `bytes=0-99`; vazio = tudo) como
/// bytes crus. Retorna o tamanho, `-(tamanho)` se o buffer for pequeno, ou -1.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_get(
    handle: i64,
    bucket: *const c_char,
    key: *const c_char,
    range: *const c_char,
    buf: *mut u8,
    cap: i64,
) -> i64 {
    guard(-1, || {
        let (bucket, key, range) = unsafe { (text(bucket)?, text(key)?, text(range)?) };
        let inst = get(handle)?;
        let data = inst.block_on(inst.store.get(bucket, key, range))?;
        Ok(unsafe { write_bytes(buf, cap, &data) })
    })
}

/// Grava o objeto no arquivo `path` (em stream). Retorna os bytes gravados ou -1.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_get_file(
    handle: i64,
    bucket: *const c_char,
    key: *const c_char,
    path: *const c_char,
    range: *const c_char,
) -> i64 {
    guard(-1, || {
        let (bucket, key, path, range) = unsafe { (text(bucket)?, text(key)?, text(path)?, text(range)?) };
        let inst = get(handle)?;
        Ok(inst.block_on(inst.store.get_file(bucket, key, path, range))? as i64)
    })
}

/// `{"key", "size", "etag", "modified", "content_type", "metadata"}`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_head(handle: i64, bucket: *const c_char, key: *const c_char, buf: *mut u8, cap: i64) -> i64 {
    guard(-1, || {
        let (bucket, key) = unsafe { (text(bucket)?, text(key)?) };
        let inst = get(handle)?;
        let v = inst.block_on(inst.store.head(bucket, key))?;
        Ok(unsafe { write_json(buf, cap, v) })
    })
}

/// 1 se o objeto existe, 0 se não, -1 em erro.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_exists(handle: i64, bucket: *const c_char, key: *const c_char) -> i32 {
    flag(|| {
        let (bucket, key) = unsafe { (text(bucket)?, text(key)?) };
        let inst = get(handle)?;
        inst.block_on(inst.store.exists(bucket, key))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_delete(handle: i64, bucket: *const c_char, key: *const c_char) -> i32 {
    status(|| {
        let (bucket, key) = unsafe { (text(bucket)?, text(key)?) };
        let inst = get(handle)?;
        inst.block_on(inst.store.delete(bucket, key))
    })
}

/// Copia `src_bucket/src_key` para `dst_bucket/dst_key`. Escreve `{"etag"}`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fs3_copy(
    handle: i64,
    src_bucket: *const c_char,
    src_key: *const c_char,
    dst_bucket: *const c_char,
    dst_key: *const c_char,
    buf: *mut u8,
    cap: i64,
) -> i64 {
    guard(-1, || {
        let (sb, sk, db, dk) = unsafe { (text(src_bucket)?, text(src_key)?, text(dst_bucket)?, text(dst_key)?) };
        let inst = get(handle)?;
        let v = inst.block_on(inst.store.copy(sb, sk, db, dk))?;
        Ok(unsafe { write_json(buf, cap, v) })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn call(f: impl FnOnce(*mut u8, i64) -> i64) -> (i64, String) {
        let mut buf = vec![0u8; 1 << 16];
        let n = f(buf.as_mut_ptr(), buf.len() as i64);
        let s = if n >= 0 { String::from_utf8(buf[..n as usize].to_vec()).unwrap() } else { String::new() };
        (n, s)
    }

    fn c(s: &str) -> CString {
        CString::new(s).unwrap()
    }

    fn last_error() -> String {
        call(|b, n| unsafe { fs3_last_error(b, n) }).1
    }

    fn temp_root(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("fogus3s-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn version() {
        let (n, v) = call(|b, c| unsafe { fs3_version(b, c) });
        assert_eq!(v, env!("CARGO_PKG_VERSION"));
        assert_eq!(n as usize, v.len());
        assert_eq!(fs3_close(999_999), -1);
        assert!(last_error().contains("invalid handle"));
    }

    #[test]
    fn store_roundtrip() {
        let root = temp_root("store");
        let h = unsafe { fs3_open(c(root.to_str().unwrap()).as_ptr()) };
        assert!(h > 0, "{}", last_error());
        assert!(root.is_dir());

        let bucket = c("docs");
        assert_eq!(unsafe { fs3_bucket_exists(h, bucket.as_ptr()) }, 0);
        assert_eq!(unsafe { fs3_bucket_create(h, bucket.as_ptr()) }, 0, "{}", last_error());
        assert_eq!(unsafe { fs3_bucket_exists(h, bucket.as_ptr()) }, 1);
        assert_eq!(unsafe { fs3_bucket_create(h, bucket.as_ptr()) }, -1);
        assert!(last_error().starts_with("BucketAlreadyExists"), "{}", last_error());

        let (_, j) = call(|b, n| unsafe { fs3_buckets(h, b, n) });
        let v: Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v[0]["name"], "docs");

        let key = c("a/hello.txt");
        let opts = c(r#"{"content_type": "text/plain", "metadata": {"Autor": "fogu"}}"#);
        let data = b"hello s3";
        let (n, j) = call(|b, n| unsafe {
            fs3_put(h, bucket.as_ptr(), key.as_ptr(), data.as_ptr(), data.len() as i64, opts.as_ptr(), b, n)
        });
        assert!(n > 0, "{}", last_error());
        let v: Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v["size"], 8);
        assert!(v["etag"].as_str().unwrap().len() >= 32);

        assert_eq!(unsafe { fs3_exists(h, bucket.as_ptr(), key.as_ptr()) }, 1);
        let (_, j) = call(|b, n| unsafe { fs3_head(h, bucket.as_ptr(), key.as_ptr(), b, n) });
        let v: Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v["size"], 8);
        assert_eq!(v["content_type"], "text/plain");
        assert_eq!(v["metadata"]["autor"], "fogu");

        let empty = c("");
        let (n, s) = call(|b, n| unsafe { fs3_get(h, bucket.as_ptr(), key.as_ptr(), empty.as_ptr(), b, n) });
        assert_eq!(n, 8);
        assert_eq!(s, "hello s3");
        let range = c("bytes=0-4");
        let (_, s) = call(|b, n| unsafe { fs3_get(h, bucket.as_ptr(), key.as_ptr(), range.as_ptr(), b, n) });
        assert_eq!(s, "hello");
        let mut tiny = [0u8; 2];
        assert_eq!(unsafe { fs3_get(h, bucket.as_ptr(), key.as_ptr(), empty.as_ptr(), tiny.as_mut_ptr(), 2) }, -8);

        let dst = c("b/copy.txt");
        let (n, _) = call(|b, n| unsafe { fs3_copy(h, bucket.as_ptr(), key.as_ptr(), bucket.as_ptr(), dst.as_ptr(), b, n) });
        assert!(n > 0, "{}", last_error());

        let prefix = c("a/");
        let (_, j) = call(|b, n| unsafe {
            fs3_list(h, bucket.as_ptr(), prefix.as_ptr(), empty.as_ptr(), 0, empty.as_ptr(), b, n)
        });
        let v: Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v["objects"].as_array().unwrap().len(), 1);
        assert_eq!(v["objects"][0]["key"], "a/hello.txt");
        let slash = c("/");
        let (_, j) = call(|b, n| unsafe {
            fs3_list(h, bucket.as_ptr(), empty.as_ptr(), slash.as_ptr(), 0, empty.as_ptr(), b, n)
        });
        let v: Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v["prefixes"].as_array().unwrap().len(), 2);

        let file = root.join("out.txt");
        let file_c = c(file.to_str().unwrap());
        assert_eq!(unsafe { fs3_get_file(h, bucket.as_ptr(), dst.as_ptr(), file_c.as_ptr(), empty.as_ptr()) }, 8);
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "hello s3");
        let key2 = c("c/from_file.txt");
        let (n, _) = call(|b, n| unsafe {
            fs3_put_file(h, bucket.as_ptr(), key2.as_ptr(), file_c.as_ptr(), empty.as_ptr(), b, n)
        });
        assert!(n > 0, "{}", last_error());

        let missing = c("nope");
        let (n, _) = call(|b, n| unsafe { fs3_get(h, bucket.as_ptr(), missing.as_ptr(), empty.as_ptr(), b, n) });
        assert_eq!(n, -1);
        assert!(last_error().starts_with("NoSuchKey"), "{}", last_error());
        assert_eq!(unsafe { fs3_exists(h, bucket.as_ptr(), missing.as_ptr()) }, 0);

        assert_eq!(unsafe { fs3_bucket_delete(h, bucket.as_ptr(), 0) }, -1);
        assert!(last_error().starts_with("BucketNotEmpty"), "{}", last_error());
        for k in [&key, &dst, &key2] {
            assert_eq!(unsafe { fs3_delete(h, bucket.as_ptr(), k.as_ptr()) }, 0, "{}", last_error());
        }
        assert_eq!(unsafe { fs3_bucket_delete(h, bucket.as_ptr(), 0) }, 0, "{}", last_error());
        assert_eq!(unsafe { fs3_bucket_exists(h, bucket.as_ptr()) }, 0);

        // force: apaga o bucket com objetos dentro.
        assert_eq!(unsafe { fs3_bucket_create(h, bucket.as_ptr()) }, 0, "{}", last_error());
        let (n, _) = call(|b, n| unsafe {
            fs3_put(h, bucket.as_ptr(), key.as_ptr(), data.as_ptr(), data.len() as i64, empty.as_ptr(), b, n)
        });
        assert!(n > 0, "{}", last_error());
        assert_eq!(unsafe { fs3_bucket_delete(h, bucket.as_ptr(), 1) }, 0, "{}", last_error());
        assert_eq!(unsafe { fs3_bucket_exists(h, bucket.as_ptr()) }, 0);
        assert_eq!(fs3_close(h), 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    static EVENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());

    extern "C" fn on_event(json: *const u8, len: usize, _b: *const u8, _bl: usize) -> i32 {
        let s = unsafe { std::str::from_utf8(std::slice::from_raw_parts(json, len)) }.unwrap().to_string();
        EVENTS.lock().unwrap().push(s);
        1
    }

    #[test]
    fn listen_and_serve() {
        let root = temp_root("listen");
        let cfg = format!(
            r#"{{"bind": "127.0.0.1:0", "root": {}, "access_key": "ak", "secret_key": "sk"}}"#,
            Value::from(root.to_str().unwrap())
        );
        let h = unsafe { fs3_listen(c(&cfg).as_ptr()) };
        assert!(h > 0, "{}", last_error());
        let (_, j) = call(|b, n| unsafe { fs3_info(h, b, n) });
        let info: Value = serde_json::from_str(&j).unwrap();
        assert_eq!(info["auth"], true);
        let port = info["port"].as_u64().unwrap();
        assert!(port > 0);

        // Primeiro evento: listening.
        assert_eq!(fs3_dispatch(h, 1000, 10, Some(on_event)), 1);
        assert!(EVENTS.lock().unwrap()[0].contains("\"listening\""));

        // Uma requisição anônima num servidor com credenciais: 403 e um evento request.
        use std::io::{Read, Write};
        let mut s = std::net::TcpStream::connect(("127.0.0.1", port as u16)).unwrap();
        s.write_all(b"GET /docs HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).unwrap();
        assert!(resp.starts_with("HTTP/1.1 403"), "{resp}");
        assert_eq!(fs3_dispatch(h, 2000, 10, Some(on_event)), 1);
        let ev: Value = serde_json::from_str(&EVENTS.lock().unwrap()[1]).unwrap();
        assert_eq!(ev["type"], "request");
        assert_eq!(ev["status"], 403);
        assert_eq!(ev["bucket"], "docs");
        assert_eq!(fs3_requests(h), 1);
        assert_eq!(fs3_close(h), 0);
        let _ = std::fs::remove_dir_all(&root);
    }
}
