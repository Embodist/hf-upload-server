# HF Upload Server

异步、高并发的 Hugging Face 上传服务：外部客户端通过 HTTP API 上传本地文件、远程 URL 或原始数据流，服务端负责下载、可选 AES 加密、批量提交到 Hugging Face 仓库，并提供队列、任务进度、SSE 实时事件和 Prometheus 监控。

同时附带一个独立 CLI（`hfpush` / `hf_push.py`），适合脚本化上传。

## 功能特性

- **三种上传入口**
  - `multipart/form-data`：一次请求上传多个文件
  - 远程 URL：支持并发下载、重试、重定向逐跳校验
  - 原始请求体流式上传：无 multipart 开销
- **仓库配置灵活**
  - 启动参数 / 配置文件 / 环境变量指定默认仓库
  - 单请求可通过 `repo_id`、`repo_type`、`revision`、`dir`、`private` 覆盖
  - 仓库不存在时可自动创建
- **批量提交，节省 commit 配额**
  - 一个任务（job）合并为一次 `HfApi.create_commit`
  - 避免“每个文件一次 commit”触发 HF 的每小时 commit 限额
- **队列与监控**
  - `/queue`：队列、并发、累计吞吐
  - `/jobs`、`/jobs/{id}`：任务与文件级状态、错误原因
  - `/jobs/{id}/events`：SSE 实时进度（含 keepalive）
  - `/metrics`：Prometheus 指标
  - `/dashboard`：内置可视化监控面板
- **错误可观测**
  - 每个文件独立 `status` / `error`
  - 任务级 `errors` 汇总，保留 HF 返回的 Request ID 与错误正文
  - `/errors`：HTTP 状态码、任务状态、常见 HF 错误说明
- **安全**
  - `X-API-Key` / `Authorization: Bearer` 鉴权
  - 目录穿越、非法文件名、非法 `repo_id` 校验
  - URL SSRF 防护：拒绝内网 / 回环 / 链路本地地址，重定向逐跳校验
  - 请求级 token 覆盖默认关闭
- **内存友好**
  - 下载、接收、加密、上传全程按 1MB 分块流式处理
  - 临时文件在任务结束后立即删除，默认不落持久化副本

## 架构

```
Client (curl / Python / dashboard)
        │  multipart / JSON URL / raw stream
        ▼
FastAPI  ──►  Job Queue (asyncio, in-memory)
        │           │
        │           ├─ URL 下载（并发 + 重试）──► 临时文件
        │           ├─ 可选 AES-256-GCM 加密 ──► 临时 .aes
        │           └─ 单任务一次 create_commit ──► Hugging Face
        │
        └─ /queue /jobs /jobs/{id}/events /metrics /dashboard
```

## 项目结构

```
.
├── hf_upload_server.py          # FastAPI 服务
├── hf_push.py                   # 独立 CLI：上传 / AES 加解密
├── hfpush                       # CLI 启动包装脚本
├── server_config.example.json   # 配置模板
├── .env.example                 # 环境变量模板
├── requirements.txt
├── requirements-dev.txt
├── pytest.ini
├── Dockerfile
├── examples/
│   ├── curl.sh                  # curl 调用示例
│   └── client.py                # Python 客户端示例
└── tests/
    └── test_server.py           # 不访问 HF 的端到端 mock 测试
```

## 快速开始

要求 Python 3.10+。

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp server_config.example.json server_config.json
# 编辑 server_config.json 里的 repo_id / default_dir / private
# token、api_key 建议通过环境变量注入

export HF_TOKEN='hf_xxxxxxxxxxxxxxxxxxxx'        # 需要 write 权限
export HF_SERVER_API_KEY='change-me'             # 强烈建议设置

python hf_upload_server.py --host 0.0.0.0 --port 8000
```

启动后：

- 监控面板：`http://127.0.0.1:8000/dashboard`
- 交互式 API 文档：`http://127.0.0.1:8000/docs`
- 健康检查：`GET /health`

也可以不用配置文件，直接用启动参数覆盖：

```bash
python hf_upload_server.py \
  --repo-id your-name/your-dataset \
  --repo-type dataset \
  --default-dir uploads \
  --host 0.0.0.0 --port 8000
```

启动参数：`--config --host --port --repo-id --repo-type --revision --token --default-dir --api-key --log-level`。

> 任务队列保存在进程内存中，因此必须使用单 worker。需要多进程/多机部署时，请改用 Redis 等外部队列。

## 配置项

`server_config.json` 支持 `${ENV_VAR}` 形式的环境变量展开。

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `repo_id` | 无 | 默认仓库，如 `user/dataset` |
| `repo_type` | `dataset` | `dataset` / `model` / `space` |
| `revision` | `main` | 目标分支 |
| `token` | `${HF_TOKEN}` | HF token；也可用 `HF_TOKEN` 环境变量或 `hf auth login` 缓存 |
| `default_dir` | 空 | 仓库内默认目录 |
| `private` | `true` | 自动创建仓库时的可见性；已存在仓库不会被改动 |
| `auto_create_repo` | `true` | 仓库不存在时是否自动创建 |
| `max_concurrent_uploads` | `4` | 单次 commit 内的上传线程数 |
| `max_concurrent_commits` | `2` | 同时进行的 commit 数 |
| `max_concurrent_downloads` | `8` | 同时下载的 URL 数 |
| `max_file_size` | `0` | 单文件上限（字节），`0` 表示不限制 |
| `temp_dir` | 系统临时目录 | 临时文件根目录 |
| `keep_temp` | `false` | 任务结束后是否保留临时文件 |
| `encrypt_default_password` | `123456` | 请求开启加密但未传密码时使用 |
| `api_key` | `${HF_SERVER_API_KEY}` | 服务端鉴权 Key；为空则不鉴权（不建议公网使用） |
| `allow_token_override` | `false` | 是否允许请求级 `token` 覆盖 |
| `allow_private_urls` | `false` | 是否允许 URL 指向内网（仅可信网络开启） |
| `allowed_hosts` | `[]` | URL 下载主机白名单；为空表示不限制 |
| `cors_origins` | `[]` | CORS 允许来源 |
| `job_history_size` | `500` | 内存中保留的任务数上限 |
| `sse_interval` | `1.0` | SSE 推送最小间隔（秒） |
| `connect_timeout` | `15` | URL 下载连接超时（秒） |
| `read_timeout` | `120` | URL 下载读取超时（秒） |
| `download_retries` | `3` | URL 下载重试次数 |

## 鉴权

配置 `api_key` 后，所有管理/上传接口都需要：

```http
X-API-Key: change-me
```

或：

```http
Authorization: Bearer change-me
```

`/health`、`/`、`/dashboard`、`/errors`、`/docs` 为公开页面；页面内的 API 请求仍需 Key。

## API 使用

### 1. 表单上传（多文件）

```bash
curl -X POST "http://127.0.0.1:8000/upload/file" \
  -H "X-API-Key: $HF_SERVER_API_KEY" \
  -F "files=@a.bin" \
  -F "files=@b.bin" \
  -F "dir=data/2026" \
  -F "wait=true"
```

可选表单字段：`repo_id`、`repo_type`、`revision`、`private`、`encrypt`、`password`、`commit_message`、`wait`、`token`（仅当 `allow_token_override=true`）。

`wait=true` 返回 `200` 和任务结果；默认 `wait=false` 返回 `202` 和 `job_id`。

### 2. URL 上传

```bash
curl -X POST "http://127.0.0.1:8000/upload/url" \
  -H "X-API-Key: $HF_SERVER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "urls": ["https://example.com/a.zip", "https://example.com/b.zip"],
    "dir": "data",
    "encrypt": true,
    "password": "123456",
    "wait": false
  }'
```

`filename` 仅在单个 URL 时生效；多 URL 时从 URL 路径自动推导，重名自动添加 `-1`、`-2` 后缀。

### 3. 原始流上传

```bash
curl -X POST "http://127.0.0.1:8000/upload/stream?filename=a.bin&dir=data&wait=true" \
  -H "X-API-Key: $HF_SERVER_API_KEY" \
  --data-binary @a.bin
```

### 4. 请求级覆盖仓库

```bash
curl -X POST "http://127.0.0.1:8000/upload/file" \
  -H "X-API-Key: $HF_SERVER_API_KEY" \
  -F "files=@a.bin" \
  -F "repo_id=other-name/other-dataset" \
  -F "repo_type=dataset" \
  -F "revision=main" \
  -F "dir=incoming" \
  -F "private=false"
```

### 5. 任务与队列监控

```bash
# 最近任务
curl -H "X-API-Key: $HF_SERVER_API_KEY" "http://127.0.0.1:8000/jobs?limit=20"
curl -H "X-API-Key: $HF_SERVER_API_KEY" "http://127.0.0.1:8000/jobs?status=failed"

# 单个任务（含文件级状态与 errors）
curl -H "X-API-Key: $HF_SERVER_API_KEY" "http://127.0.0.1:8000/jobs/<job_id>"

# 队列与并发快照
curl -H "X-API-Key: $HF_SERVER_API_KEY" "http://127.0.0.1:8000/queue"

# SSE 实时事件（任务结束后自动关闭）
curl -N -H "X-API-Key: $HF_SERVER_API_KEY" \
  "http://127.0.0.1:8000/jobs/<job_id>/events"

# Prometheus 指标
curl -H "X-API-Key: $HF_SERVER_API_KEY" "http://127.0.0.1:8000/metrics"
```

任务响应中的关键字段：

- `status`：`queued` / `running` / `success` / `partial` / `failed` / `cancelled`
- `progress`：`completed_files`、`prepared_files`、`failed_files`、`total_files`、加权 `percent`
- `errors`：任务内所有失败项的 `{path_in_repo, error}` 汇总
- `files[]`：每个文件的 `status`、`size`、`sha256`、`commit_id`、`error`

### 6. 其他接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/files?repo_id=...&repo_type=...&revision=...` | 列出仓库文件 |
| GET | `/whoami` | 当前 HF 身份 |
| GET | `/config` | 生效配置（token 已脱敏） |
| POST | `/repos` | 创建仓库（`{"repo_id","repo_type","private"}`） |
| GET | `/errors` | 错误码与状态说明 |
| GET | `/dashboard` | 监控面板 |
| GET | `/metrics` | Prometheus 指标 |
| GET | `/health` | 健康检查 |

## 错误排查

### HTTP 状态码

| 状态码 | 含义 |
|---|---|
| `400` | 参数非法：目录穿越、非法 `repo_id`、URL 非 http(s)、URL 指向内网 |
| `401` | API Key 缺失或错误 |
| `403` | 主机不在 `allowed_hosts`，或请求级 token 覆盖被禁用 |
| `404` | 任务不存在 |
| `413` | 文件超过 `max_file_size` |
| `422` | 请求字段校验失败，详见响应 `detail` |
| `503` | 服务端未配置 HF token |

### 任务/文件状态

| 状态 | 含义 |
|---|---|
| `queued` | 已入队 |
| `running` | 执行中 |
| `success` | 成功 |
| `partial` | 部分成功，查看 `errors` / `files[].error` |
| `failed` | 全部失败或 commit 失败 |
| `cancelled` | 服务关闭时取消 |
| `pending` / `downloading` / `saved` / `downloaded` / `encrypting` / `prepared` / `uploading` | 文件级阶段 |

### 常见 HF 错误

| 错误 | 处理 |
|---|---|
| `401 Invalid user token` | 检查 `HF_TOKEN` 是否有效 |
| `403` | token 缺少 write 权限，或对目标仓库无权限 |
| `404` 仓库不存在 | 开启 `auto_create_repo`，或先 `POST /repos` |
| `429 rate limit for repository commit creation` | HF 限制 **128 次 commit/小时**。把多个文件放进同一任务以减少 commit；等待额度重置或升级账号。错误正文包含 HF Request ID |
| `5xx` | HF 服务端错误；服务会自动重试 3 次 |

## 性能与并发

- **一个任务 = 一次 commit**：同一请求的多个文件在 `create_commit` 中批量提交，显著减少 commit 次数。
- `max_concurrent_uploads`：`create_commit` 内部的上传线程数。
- `max_concurrent_commits`：同时执行的 commit 数；多个任务并行时受此限制。
- `max_concurrent_downloads`：URL 下载并发数，与上传解耦。
- 大文件由 `hf_xet` 自动分块/去重；建议安装 `hf_xet`（已包含在依赖中）。
- 单进程内存队列，单 worker 运行；需要水平扩展时请引入外部队列（Redis 等）。

## 内存与落盘

- 接收、下载、加密均按 **1MB 分块**流式处理，不会把整个文件读入内存。
- multipart 解析由 Starlette 负责，大文件会落到其临时文件；本服务随后写入自己的任务临时目录。
- 任务结束后（成功/失败/取消）默认立即删除临时目录；`keep_temp=true` 可保留用于排障。
- `temp_dir` 可指向容量较大的磁盘，避免系统盘压力。
- 服务不做任何持久化本地副本；需要长期保存请自行配置对象存储。

## 安全建议

1. 公网部署务必设置 `api_key`，并通过 HTTPS 反向代理（Nginx / Caddy）暴露。
2. 保持 `allow_private_urls=false`；确需内网 URL 时，优先使用 `allowed_hosts` 白名单。
3. 保持 `allow_token_override=false`，避免客户端把 token 传入服务。
4. 不要把 `.env`、`.hf_token`、真实的 `server_config.json` 提交到仓库。
5. Dashboard 会把 API Key 保存在浏览器 `localStorage`，请仅在可信终端使用。

## Docker

```bash
docker build -t hf-upload-server .
docker run --rm -p 8000:8000 \
  -e HF_TOKEN='hf_xxx' \
  -e HF_SERVER_API_KEY='change-me' \
  hf-upload-server \
  --repo-id your-name/your-dataset --repo-type dataset --default-dir uploads
```

需要配置文件时挂载：

```bash
docker run --rm -p 8000:8000 \
  -e HF_TOKEN='hf_xxx' -e HF_SERVER_API_KEY='change-me' \
  -v "$PWD/server_config.json:/app/server_config.json:ro" \
  hf-upload-server
```

## 测试

测试使用 mock commit，不访问 Hugging Face，也不会产生真实上传：

```bash
pip install -r requirements-dev.txt
pytest -q
```

覆盖：鉴权、表单多文件去重、URL 下载、流上传、AES 加密与解密、异步任务轮询、SSE、队列/指标、错误目录、路径穿越、SSRF、临时文件清理。

## 独立 CLI

```bash
# 上传本地文件 / HTTP 文件，可选 AES 加密
./hfpush upload -s ./books.csv -r your-name/your-dataset -t dataset --encrypt
./hfpush upload -s https://example.com/a.zip -r your-name/your-dataset --encrypt --password 123456

# 解密
./hfpush decrypt -s books.csv.aes -o books.csv --password 123456

# 登录 / 查看身份
./hfpush login
./hfpush whoami
```

`hfpush` 会优先使用同目录 `.venv`，否则使用 `python3`。
