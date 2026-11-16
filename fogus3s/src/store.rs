//! Armazenamento (s3s-fs) compartilhado entre o servidor HTTP e as operações
//! diretas do Fogu, mais os helpers que traduzem os DTOs da s3s para JSON.
//!
//! O `S3ServiceBuilder` consome o `impl S3`, e o `FileSystem` da s3s-fs tem
//! um contador de arquivos temporários por instância (`.tmp.N.internal.part`):
//! duas instâncias sobre a mesma pasta colidiriam. Por isso o servidor e as
//! operações diretas usam o mesmo `FileSystem` por trás deste `Store`, que
//! implementa `S3` delegando as operações que a s3s-fs suporta.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use http::{Extensions, HeaderMap, Method, Uri};
use s3s::dto::*;
use s3s::{S3, S3Error, S3Request, S3Response, S3Result};
use s3s_fs::FileSystem;
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

#[derive(Clone)]
pub struct Store(pub Arc<FileSystem>);

// O `#[async_trait]` precisa ver o bloco `impl` já expandido, por isso a macro
// gera o bloco inteiro (métodos soltos dentro dele não seriam transformados).
macro_rules! delegate {
    ($($name:ident: $input:ident => $output:ident),* $(,)?) => {
        #[async_trait::async_trait]
        impl S3 for Store {
            $(
                async fn $name(&self, req: S3Request<$input>) -> S3Result<S3Response<$output>> {
                    self.0.$name(req).await
                }
            )*
        }
    };
}

delegate!(
        create_bucket: CreateBucketInput => CreateBucketOutput,
        copy_object: CopyObjectInput => CopyObjectOutput,
        delete_bucket: DeleteBucketInput => DeleteBucketOutput,
        delete_object: DeleteObjectInput => DeleteObjectOutput,
        delete_objects: DeleteObjectsInput => DeleteObjectsOutput,
        get_bucket_location: GetBucketLocationInput => GetBucketLocationOutput,
        get_object: GetObjectInput => GetObjectOutput,
        head_bucket: HeadBucketInput => HeadBucketOutput,
        head_object: HeadObjectInput => HeadObjectOutput,
        list_buckets: ListBucketsInput => ListBucketsOutput,
        list_objects: ListObjectsInput => ListObjectsOutput,
        list_objects_v2: ListObjectsV2Input => ListObjectsV2Output,
        put_object: PutObjectInput => PutObjectOutput,
        create_multipart_upload: CreateMultipartUploadInput => CreateMultipartUploadOutput,
        upload_part: UploadPartInput => UploadPartOutput,
        upload_part_copy: UploadPartCopyInput => UploadPartCopyOutput,
        list_parts: ListPartsInput => ListPartsOutput,
        complete_multipart_upload: CompleteMultipartUploadInput => CompleteMultipartUploadOutput,
        abort_multipart_upload: AbortMultipartUploadInput => AbortMultipartUploadOutput,
);

/// Um `S3Request` "interno" (sem HTTP, anônimo) para chamar o backend direto.
fn request<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: Method::GET,
        uri: Uri::default(),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// `Codigo: mensagem` (ex. `NoSuchKey: The specified key does not exist.`),
/// para o Fogu poder testar o código com `starts_with`.
pub fn err_text(e: &S3Error) -> String {
    let code = e.code().as_str();
    match e.message().or_else(|| e.code().default_message()) {
        Some(m) => format!("{code}: {m}"),
        None => code.to_string(),
    }
}

fn is_code(e: &S3Error, code: &str) -> bool {
    e.code().as_str() == code
}

fn timestamp(t: Option<&Timestamp>) -> Value {
    let Some(t) = t else { return Value::Null };
    let mut out = Vec::new();
    match t.format(TimestampFormat::DateTime, &mut out) {
        Ok(()) => Value::String(String::from_utf8_lossy(&out).into_owned()),
        Err(_) => Value::Null,
    }
}

fn etag(e: Option<&ETag>) -> Value {
    match e {
        Some(e) => Value::String(e.value().to_string()),
        None => Value::Null,
    }
}

fn build<T, E: std::fmt::Display>(r: Result<T, E>) -> Result<T, String> {
    r.map_err(|e| e.to_string())
}

async fn run<T>(fut: impl Future<Output = S3Result<S3Response<T>>>) -> Result<T, String> {
    fut.await.map(|r| r.output).map_err(|e| err_text(&e))
}

/// Opções de `put`/`put_file`: `{ content_type, cache_control, content_encoding,
/// content_disposition, metadata: { chave: valor } }`.
fn put_input(bucket: &str, key: &str, body: StreamingBlob, len: Option<i64>, opts: &Value) -> Result<PutObjectInput, String> {
    let text = |k: &str| opts.get(k).and_then(Value::as_str).map(str::to_string);
    let mut b = PutObjectInput::builder()
        .bucket(bucket.to_string())
        .key(key.to_string())
        .body(Some(body))
        .content_length(len)
        .content_type(text("content_type"))
        .cache_control(text("cache_control"))
        .content_encoding(text("content_encoding"))
        .content_disposition(text("content_disposition"));
    if let Some(m) = opts.get("metadata").and_then(Value::as_object) {
        let map: Metadata = m
            .iter()
            .map(|(k, v)| (k.to_lowercase(), v.as_str().map_or_else(|| v.to_string(), str::to_string)))
            .collect();
        b = b.metadata(Some(map));
    }
    build(b.build())
}

impl Store {
    pub fn open(root: &Path) -> Result<Store, String> {
        std::fs::create_dir_all(root).map_err(|e| format!("cannot create root '{}': {e}", root.display()))?;
        let fs = FileSystem::new(root).map_err(|e| format!("cannot open root '{}': {e:?}", root.display()))?;
        Ok(Store(Arc::new(fs)))
    }

    /// `[{ "name", "created" }]`
    pub async fn buckets(&self) -> Result<Value, String> {
        let out = run(self.0.list_buckets(request(build(ListBucketsInput::builder().build())?))).await?;
        let list: Vec<Value> = out
            .buckets
            .unwrap_or_default()
            .iter()
            .map(|b| json!({ "name": b.name, "created": timestamp(b.creation_date.as_ref()) }))
            .collect();
        Ok(Value::Array(list))
    }

    pub async fn bucket_create(&self, name: &str) -> Result<(), String> {
        let input = build(CreateBucketInput::builder().bucket(name.to_string()).build())?;
        run(self.0.create_bucket(request(input))).await.map(|_| ())
    }

    /// Apaga o bucket. A s3s-fs apaga a pasta inteira (`remove_dir_all`), ao
    /// contrário do S3; sem `force`, um bucket com objetos é recusado com
    /// `BucketNotEmpty`, como no S3.
    pub async fn bucket_delete(&self, name: &str, force: bool) -> Result<(), String> {
        if !force {
            let listing = self.list(name, "", "", 1, "").await?;
            if listing["objects"].as_array().is_some_and(|l| !l.is_empty()) {
                return Err(format!("BucketNotEmpty: bucket '{name}' is not empty (delete the objects first, or force)"));
            }
        }
        let input = build(DeleteBucketInput::builder().bucket(name.to_string()).build())?;
        run(self.0.delete_bucket(request(input))).await.map(|_| ())
    }

    pub async fn bucket_exists(&self, name: &str) -> Result<bool, String> {
        let input = build(HeadBucketInput::builder().bucket(name.to_string()).build())?;
        match self.0.head_bucket(request(input)).await {
            Ok(_) => Ok(true),
            Err(e) if is_code(&e, "NoSuchBucket") || is_code(&e, "NotFound") => Ok(false),
            Err(e) => Err(err_text(&e)),
        }
    }

    /// `{ "objects": [{ key, size, etag, modified }], "prefixes": [..],
    /// "truncated": bool, "next": token | null }`
    pub async fn list(&self, bucket: &str, prefix: &str, delimiter: &str, max: i64, token: &str) -> Result<Value, String> {
        let opt = |s: &str| if s.is_empty() { None } else { Some(s.to_string()) };
        let input = build(
            ListObjectsV2Input::builder()
                .bucket(bucket.to_string())
                .prefix(opt(prefix))
                .delimiter(opt(delimiter))
                .max_keys(if max > 0 { Some(max.min(i32::MAX as i64) as i32) } else { None })
                .continuation_token(opt(token))
                .build(),
        )?;
        let out = run(self.0.list_objects_v2(request(input))).await?;
        let objects: Vec<Value> = out
            .contents
            .unwrap_or_default()
            .iter()
            .map(|o| {
                json!({
                    "key": o.key,
                    "size": o.size.unwrap_or(0),
                    "etag": etag(o.e_tag.as_ref()),
                    "modified": timestamp(o.last_modified.as_ref()),
                })
            })
            .collect();
        let prefixes: Vec<Value> = out
            .common_prefixes
            .unwrap_or_default()
            .iter()
            .filter_map(|p| p.prefix.clone().map(Value::String))
            .collect();
        Ok(json!({
            "objects": objects,
            "prefixes": prefixes,
            "truncated": out.is_truncated.unwrap_or(false),
            "next": out.next_continuation_token,
        }))
    }

    /// Grava `data` em `bucket/key`. `{ "etag", "size" }`
    pub async fn put(&self, bucket: &str, key: &str, data: Bytes, opts: &Value) -> Result<Value, String> {
        let len = data.len() as i64;
        let input = put_input(bucket, key, StreamingBlob::from_bytes(data), Some(len), opts)?;
        let out = run(self.0.put_object(request(input))).await?;
        Ok(json!({ "etag": etag(out.e_tag.as_ref()), "size": len }))
    }

    /// Envia um arquivo do disco em stream (sem carregar na memória).
    pub async fn put_file(&self, bucket: &str, key: &str, path: &str, opts: &Value) -> Result<Value, String> {
        let file = tokio::fs::File::open(path).await.map_err(|e| format!("cannot open '{path}': {e}"))?;
        let len = file.metadata().await.map_err(|e| e.to_string())?.len() as i64;
        let stream = tokio_util::io::ReaderStream::new(file);
        let input = put_input(bucket, key, StreamingBlob::wrap(stream), Some(len), opts)?;
        let out = run(self.0.put_object(request(input))).await?;
        Ok(json!({ "etag": etag(out.e_tag.as_ref()), "size": len }))
    }

    async fn get_output(&self, bucket: &str, key: &str, range: &str) -> Result<GetObjectOutput, String> {
        let range = if range.is_empty() {
            None
        } else {
            Some(Range::parse(range).map_err(|e| format!("invalid range '{range}': {e}"))?)
        };
        let input = build(GetObjectInput::builder().bucket(bucket.to_string()).key(key.to_string()).range(range).build())?;
        run(self.0.get_object(request(input))).await
    }

    /// Lê o objeto inteiro (ou a faixa `range`, como `bytes=0-99`).
    pub async fn get(&self, bucket: &str, key: &str, range: &str) -> Result<Vec<u8>, String> {
        let out = self.get_output(bucket, key, range).await?;
        let mut buf = Vec::with_capacity(out.content_length.unwrap_or(0).max(0) as usize);
        if let Some(mut body) = out.body {
            while let Some(chunk) = body.next().await {
                buf.extend_from_slice(&chunk.map_err(|e| e.to_string())?);
            }
        }
        Ok(buf)
    }

    /// Grava o objeto num arquivo do disco em stream. Devolve os bytes gravados.
    pub async fn get_file(&self, bucket: &str, key: &str, path: &str, range: &str) -> Result<u64, String> {
        let out = self.get_output(bucket, key, range).await?;
        if let Some(parent) = Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await.map_err(|e| e.to_string())?;
        }
        let mut file = tokio::fs::File::create(path).await.map_err(|e| format!("cannot create '{path}': {e}"))?;
        let mut written = 0u64;
        if let Some(mut body) = out.body {
            while let Some(chunk) = body.next().await {
                let chunk = chunk.map_err(|e| e.to_string())?;
                file.write_all(&chunk).await.map_err(|e| e.to_string())?;
                written += chunk.len() as u64;
            }
        }
        file.flush().await.map_err(|e| e.to_string())?;
        Ok(written)
    }

    /// `{ key, size, etag, modified, content_type, metadata }`
    pub async fn head(&self, bucket: &str, key: &str) -> Result<Value, String> {
        let input = build(HeadObjectInput::builder().bucket(bucket.to_string()).key(key.to_string()).build())?;
        let out = run(self.0.head_object(request(input))).await?;
        Ok(json!({
            "key": key,
            "size": out.content_length.unwrap_or(0),
            "etag": etag(out.e_tag.as_ref()),
            "modified": timestamp(out.last_modified.as_ref()),
            "content_type": out.content_type,
            "metadata": out.metadata.unwrap_or_default(),
        }))
    }

    pub async fn exists(&self, bucket: &str, key: &str) -> Result<bool, String> {
        let input = build(HeadObjectInput::builder().bucket(bucket.to_string()).key(key.to_string()).build())?;
        match self.0.head_object(request(input)).await {
            Ok(_) => Ok(true),
            Err(e) if is_code(&e, "NoSuchKey") || is_code(&e, "NotFound") || is_code(&e, "NoSuchBucket") => Ok(false),
            Err(e) => Err(err_text(&e)),
        }
    }

    pub async fn delete(&self, bucket: &str, key: &str) -> Result<(), String> {
        let input = build(DeleteObjectInput::builder().bucket(bucket.to_string()).key(key.to_string()).build())?;
        run(self.0.delete_object(request(input))).await.map(|_| ())
    }

    /// Copia dentro do armazenamento. `{ "etag" }`
    pub async fn copy(&self, src_bucket: &str, src_key: &str, dst_bucket: &str, dst_key: &str) -> Result<Value, String> {
        let source = CopySource::Bucket { bucket: src_bucket.into(), key: src_key.into(), version_id: None };
        let input = build(
            CopyObjectInput::builder()
                .bucket(dst_bucket.to_string())
                .key(dst_key.to_string())
                .copy_source(source)
                .build(),
        )?;
        let out = run(self.0.copy_object(request(input))).await?;
        let tag = out.copy_object_result.as_ref().and_then(|r| r.e_tag.as_ref());
        Ok(json!({ "etag": etag(tag) }))
    }
}
