# s3s — servidor S3 para Fogu

Uma lib Rust (`fogus3s`, baseada em [s3s](https://github.com/Nugine/s3s) e no seu backend em
disco `s3s-fs`) que roda um servidor compatível com S3, mais um módulo Fogu (`s3.fg`) que a usa
via `fogu:ffi`. Qualquer cliente S3 funciona (aws cli, MinIO client, SDKs, o `fogu:s3` do próprio
Fogu), com assinatura V4. O mesmo armazenamento também pode ser usado direto, sem HTTP.

```
fogus3s/        lib Rust (cdylib) com a API C
native/         fogus3s.dll / libfogus3s.so / libfogus3s.dylib (gerado pelo build)
s3.fg           wrapper Fogu: listen(), open(), S3Server, Store, Bucket
main.fg         exemplo: servidor + cliente fogu:s3 + log de acessos
```

<!-- fogu:plataformas -->
## Plataformas, binários e pré-requisitos

Ao importar `s3.fg`, a lib reconhece o sistema e a arquitetura
(`system.info.os`/`arch`) e já carrega o binário certo — `.dll` no Windows,
`.so` no Linux, `.dylib` no macOS. Se ele faltar, o programa não para no `import`:
o erro (com a lista de caminhos tentados) volta na primeira chamada que precisar da lib.
`nativeInfo()` mostra o que foi detectado: `{ platform, os, file, path, loaded, error }`.

| Plataforma | Arquivo | Neste repositório |
|---|---|---|
| `windows-x64` | `native/windows-x64/fogus3s.dll` | incluído |
| `windows-arm64` | `native/windows-arm64/fogus3s.dll` | — |
| `linux-x64` | `native/linux-x64/libfogus3s.so` | incluído |
| `linux-arm64` | `native/linux-arm64/libfogus3s.so` | incluído |
| `macos-x64` | `native/macos-x64/libfogus3s.dylib` | incluído |
| `macos-arm64` | `native/macos-arm64/libfogus3s.dylib` | incluído |

**Instalação** (Fogu 0.8.0+): `fogu add Edmilson-Soares/s3s@^0.2.0`. Os binários de `native/` vão
no repositório git, então a lib já funciona depois de instalada — sem Rust/cargo na máquina.

Ordem de busca: o caminho passado a `load(path)`, a variável `FOGU_S3S_LIB`,
`native/<plataforma>/`, `native/` e o build do cargo no projeto em execução, uma
cópia em `fogu_modules/` e os pacotes instalados com `fogu add` (`$FOGU_HOME/fogu_modules`).

**Pré-requisitos**

- Nenhum, em nenhum sistema (Rust puro: s3s + hyper + tokio).
- Rust ≥ 1.85 (edition 2024) para gerar os binários.

**Gerar os binários**

```sh
fogu run build:native        # Windows: native/windows-<arch>/ (build.ps1)
sh build.sh                  # Linux/macOS/WSL: o sistema atual
sh build.sh all              # Linux x64/arm64 e macOS x64/arm64 (cargo-zigbuild + zig)
```

## Callbacks do fogu:ffi (requer Fogu 0.8.0+)

A camada nativa usa `ffi.callback` (veja `_native.fg`, igual em todas as libs deste repositório):

- **Sink de saída**: a lib recebe um callback uma vez (`fs3_set_sink`) e, chamada com
  `buf = nil, cap = 0`, entrega a resposta inteira por ele (JSON ou os bytes de um objeto).
  As funções com buffer do chamador continuam funcionando (API C compatível).
- **Regras do fogu:ffi respeitadas**: um callback só roda na thread do interpretador, durante
  uma chamada FFI — as threads do servidor (tokio) nunca chamam o Fogu; elas enfileiram
  eventos e `fs3_dispatch` os entrega na thread de quem chamou. Um erro dentro de um callback
  derruba o programa, então o callback só guarda os dados e os handlers do usuário rodam depois,
  fora do frame nativo (com `?`, erros e `sleep` normais).
- **Esperas cooperativas**: uma chamada FFI bloqueante congela todas as tarefas `fg`, então as
  esperas longas são feitas em fatias de 10 ms com `time.delay(1ms)` entre elas.
- **Fogu 0.7.4+** (e 0.8.0): o código compilado chama direto as funções FFI de assinatura numérica (sem
  alocação por chamada), então sondar status/eventos num laço é barato; `fogu .` roda o projeto
  (o script `start` do `fogu.json`); os tipos `int32`, `uint8`, `float64`... valem nas assinaturas.
<!-- /fogu:plataformas -->

## Requisitos mínimos (CPU e RAM)

Medido no Windows x64 (i7-13620H, SSD) com o processo `fogu` rodando o servidor **e** o
cliente `fogu:s3` juntos — ou seja, um limite superior para o servidor sozinho:

| | CPU | RAM (working set) |
|---|---|---|
| Servidor ocioso (escutando, sem conexões) | ~0 % | ~13 MB (processo inteiro, com a lib carregada) |
| Carga: 200 PUT + 200 GET de 1 MiB (400 MiB em 19 s, ~21 MB/s) | ~0,7 núcleo (inclui o SHA-256 do cliente) | ~60 MB, pico ~85 MB privados |

- **CPU**: qualquer x64 ou arm64; **1 núcleo basta**. O runtime usa 2 threads de trabalho
  mais a de aceitação; mais núcleos só ajudam com vários clientes simultâneos. O custo por
  requisição é dominado por SHA-256/HMAC da assinatura V4 e pelo I/O de disco.
- **RAM**: reserve **128 MB livres** (mínimo) e **256 MB** para uso confortável. Os objetos são
  transmitidos em stream para o disco, então a memória **não cresce com o tamanho nem com a
  quantidade de objetos**: depende do número de conexões ativas (alguns MB por conexão, com os
  buffers do hyper) e das listagens (a s3s-fs monta a lista de um bucket em memória).
- **Disco**: a lib tem ~8,4 MB; os dados ocupam o tamanho dos objetos mais um JSON pequeno de
  metadados por objeto. Uploads multipart guardam as partes em arquivos temporários na raiz.

Cabe num Raspberry Pi ou numa VM de 1 vCPU / 512 MB.

## Build e execução

```sh
fogu run build:native       # ou ./build.ps1 (Windows) / ./build.sh (Linux/macOS)
fogu run main.fg            # S3_BIND=0.0.0.0:9000 para mudar o endereço
fogu test                   # testes (sobem servidores em portas livres)

# com qualquer cliente S3:
AWS_ACCESS_KEY_ID=fogu AWS_SECRET_ACCESS_KEY=fogusecret \
  aws --endpoint-url http://127.0.0.1:9000 s3 cp foto.jpg s3://demo/
```

## Como funciona

A lib sobe o servidor (hyper + s3s) num runtime tokio próprio. Os dados ficam em disco
(`s3s-fs`): uma pasta por bucket, um arquivo por objeto, mais um `.bucket-…metadata.json`
por objeto com content-type e metadados. Cada requisição atendida vira um evento `request`
na fila; `server.serve()` / `server.events()` / `server.next()` os recebem por callback, em
lotes. As operações diretas (`server.store`, `s3s.open()`) usam o **mesmo** backend, então
o que um cliente grava por HTTP aparece nelas e vice-versa.

```fogu
import s3s from "./s3.fg"

fn main() {
    const server = s3s.listen({ bind: "127.0.0.1:9000", root: "data", accessKey: "fogu", secretKey: "fogusecret" })?
    const demo = server.bucket("demo", true)?           // direto, sem HTTP
    demo.put("ola.txt", "Olá!", { contentType: "text/plain" })?

    const client = server.client({ bucket: "demo" })?   // fogu:s3, já apontado para o servidor
    print(client.get("ola.txt")?)

    server.serve({
        request: (ev, _s) => { print(`${ev.method} ${ev.path} -> ${ev.status}`) },
    })?
    return true, nil
}
main()
```

### Configuração (`s3s.listen(config)`)

| chave | padrão | |
|---|---|---|
| `bind` | `"127.0.0.1:9000"` | porta `0` escolhe uma livre (veja `server.port`, `server.url`) |
| `root` | `"data"` | pasta dos dados (criada se não existir) |
| `accessKey`, `secretKey` | — | credenciais aceitas (SigV4). Sem elas o servidor aceita só requisições **anônimas** (sem assinatura: `fogu:http`, curl) e recusa as assinadas — `fogu:s3`/aws cli precisam de credenciais |
| `credentials` | — | `{ accessKey: secretKey, ... }` para várias credenciais |
| `domain` | — | domínio base para buckets por host (`bucket.dominio:porta`); sem ele, só path-style |
| `events` | `true` | cada requisição vira um evento `request` |

`server.address`, `port`, `url`, `root`, `auth` vêm do evento `listening`.

### Eventos

| type | campos | |
|---|---|---|
| `listening` | `address`, `port`, `root`, `auth` | consumido por `listen()` |
| `request` | `method`, `path`, `query`, `bucket`, `key`, `status`, `ms`, `bytes_in`, `bytes_out`, `remote`, `user_agent`, `error` | uma por requisição atendida (`bucket`/`key` vêm do caminho, path-style) |
| `error` | `message` | falha ao aceitar uma conexão |

`server.events(timeout, max)` devolve a lista, `server.next(timeout)` um de cada vez,
`server.serve(handlers)` chama `handlers[type](ev, server)` (e `handlers.event` para todos)
até `stop()` ou um handler devolver `"stop"`. `server.requests()` conta as requisições.

### Armazenamento direto (`Store` / `Bucket`)

`s3s.open(root)` abre uma pasta sem servidor; `server.store` é o do servidor. Os dois têm:

| | |
|---|---|
| `buckets()` | `[{ name, created }]` |
| `createBucket(name)`, `hasBucket(name)` | |
| `deleteBucket(name, force = false)` | sem `force`, um bucket com objetos é recusado (`BucketNotEmpty`) |
| `bucket(name, create = false)` | um `Bucket` (`create` cria se faltar) |
| `list(bucket, prefix, { delimiter, max, token })` | `{ objects: [{ key, size, etag, modified }], prefixes, truncated, next }` |
| `put(bucket, key, data, { contentType, cacheControl, contentEncoding, contentDisposition, metadata })` | `{ etag, size }`; `data` é str, buffer ou lista de bytes |
| `putFile(bucket, key, path, options)` | envia um arquivo em stream |
| `get(bucket, key, { range })` / `getText(...)` | buffer / texto; `range: "bytes=0-99"` |
| `getFile(bucket, key, path, { range })` | grava num arquivo em stream; devolve os bytes |
| `head(bucket, key)` | `{ key, size, etag, modified, content_type, metadata }` |
| `exists(bucket, key)`, `delete(bucket, key)` | |
| `copy(bucket, key, dstKey, dstBucket = bucket)` | `{ etag }` |
| `close()` | |

`Bucket` tem as mesmas operações sem o nome do bucket (`has(key)` em vez de `exists`,
`remove(force)` em vez de `deleteBucket`). Os erros do S3 chegam como `Codigo: mensagem`
(`NoSuchKey: The specified key does not exist.`), então `err.message.starts_with("NoSuchKey")`
funciona.

### Cliente `fogu:s3`

`server.client(options)` é um atalho para `s3.connect({ endpoint: server.url, pathStyle: true,
accessKey, secretKey, ...options })` do módulo padrão `fogu:s3`: `put`, `get`, `getBytes`,
`head`, `exists`, `delete`, `list`, `copy`, `presign`, `listBuckets`, `createBucket`… Passe
`bucket` para fixar o bucket, ou `secretKey` diferente para testar credenciais erradas.
Só funciona num servidor com credenciais (o `fogu:s3` sempre assina).

### O que a s3s-fs implementa

Buckets (criar, listar, apagar, HeadBucket, GetBucketLocation), objetos (Put, Get com
Range e condicionais, Head, Delete, DeleteObjects, Copy, ListObjects v1/v2 com prefixo e
delimitador), uploads multipart (Create/UploadPart/UploadPartCopy/ListParts/Complete/Abort),
checksums e metadados de usuário. Versionamento, ACLs, políticas, tags, website,
notificações e afins respondem `NotImplemented`. Via HTTP, `DeleteBucket` da s3s-fs apaga a
pasta inteira (com objetos); a operação direta `deleteBucket` só faz isso com `force = true`.

## API C (`fogus3s`)

Prefixo `fs3_`. Handles `i64` (> 0) para armazenamentos e servidores; status `0`/`-1`
(mensagem em `fs3_last_error`); saídas no buffer do chamador ou pelo sink.

| função | |
|---|---|
| `fs3_version`, `fs3_last_error`, `fs3_set_sink` | |
| `fs3_open(root)` / `fs3_listen(config_json)` | handle |
| `fs3_info`, `fs3_requests`, `fs3_dispatch(h, timeout_ms, max, cb)`, `fs3_close` | |
| `fs3_buckets`, `fs3_bucket_create`, `fs3_bucket_delete(h, name, force)`, `fs3_bucket_exists` | |
| `fs3_list(h, bucket, prefix, delimiter, max, token, buf, cap)` | JSON |
| `fs3_put(h, bucket, key, data, len, opts_json, buf, cap)`, `fs3_put_file(h, bucket, key, path, opts_json, buf, cap)` | JSON `{etag, size}` |
| `fs3_get(h, bucket, key, range, buf, cap)` | bytes crus |
| `fs3_get_file(h, bucket, key, path, range)` | bytes gravados |
| `fs3_head`, `fs3_exists`, `fs3_delete`, `fs3_copy(h, sb, sk, db, dk, buf, cap)` | |

Testes da lib Rust: `cargo test --manifest-path fogus3s/Cargo.toml`.
