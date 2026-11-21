# doudouX-server

兜兜X 的服务端：**夹在 App 与 LLM 之间的流式代理网关**，同时把每一次模型调用记成可审计的 trace，
并在根路径提供一块实时看板。

## 为什么要有这一层

如果 App 直接连模型服务，可观测只能是「客户端自己上报」——异步、可能丢、正文取证困难，
而且上下文预算、裁剪这类只有端侧才知道的数据传不全。

把请求路径改成 `App → 本服务 → LLM → 本服务 → App` 之后：

- trace 在**链路中同步落库**，不依赖客户端异步上报，不存在「用户看的时候还没到」；
- 请求体与模型原始回复都能完整留存，可回放、可审计；
- 客户端提前断连能被识别成 `cancel` 而不是失败；
- 换模型、加限流、做成本统计都只需改这一层，App 不用重新发版。

## 架构

```
Android App (Base URL = http://<host>:8080/v1)
        │  POST /v1/chat/completions  (SSE)
        ▼
┌──────────────────────────────────────────────┐
│ doudouX-server                               │
│                                              │
│  LlmProxyController ─► LlmProxyService       │
│         │                    │               │
│         │                    ├─► 上游 LLM（OpenAI 兼容）
│         │                    │      逐行读、逐行回吐
│         │                    └─► TraceStore ─► SQLite
│         │                             │        │
│         │                             │        └─► TraceBroadcaster ─► SSE
│  ApiController ────────────────────────┘                │
│  static/index.html ◄──── EventSource ───────────────────┘
└──────────────────────────────────────────────┘
```

## 快速开始

前置：JDK 17+、Gradle、一个 OpenAI 兼容的上游服务（例如本地 Ollama）。

```bash
# 启动（默认 8080 端口，绑定 0.0.0.0 以便手机通过局域网访问）
gradle bootRun

# 健康检查
curl http://127.0.0.1:8080/api/health
# {"ok":true,"service":"doudou-server"}

# 浏览器打开看板
http://127.0.0.1:8080/
```

上游地址默认指向 `http://192.168.1.6:11434/v1`（作者的局域网环境），
请改成你自己的地址，或用环境变量覆盖：

```bash
OLLAMA_BASE_URL=http://127.0.0.1:11434/v1 gradle bootRun
```

手机联调时，App 的 AI Base URL 要填**电脑在局域网里的地址**，不能填 `127.0.0.1`
（那是手机自己）。

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/chat/completions` | 流式代理，SSE 逐行透传，链路中自动记 trace |
| GET | `/v1/models` | 占位，避免客户端探测时 404 |
| GET | `/api/health` | 健康检查 |
| POST | `/api/traces` | 端侧上报一条 trace（兼容旧链路） |
| GET | `/api/traces?model=&limit=` | 最近 trace 列表（不含正文，只带正文长度） |
| GET | `/api/traces/{id}` | 单条详情，含请求体与模型原始回复 |
| GET | `/api/stats` | 按模型汇总 |
| GET | `/api/stream` | SSE 实时推送，看板用 EventSource 订阅 |
| GET | `/` | 看板页面（`src/main/resources/static/index.html`） |

## 配置

`application.yml`：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `server.port` | `8080` | 监听端口 |
| `doudou.llm-base-url` | 环境变量 `OLLAMA_BASE_URL` | 上游 OpenAI 兼容地址，末尾不用带 `/chat/completions` |
| `doudou.connect-timeout-seconds` | `10` | 连接上游的超时；**流式读取不设总超时**，否则长回答会被掐断 |
| `doudou.max-traces` | `5000` | 按条数淘汰最旧 |
| `doudou.max-total-mb` | `512` | 按正文总字节淘汰最旧（`SUM(LENGTH(正文))`） |
| `spring.datasource.url` | 环境变量 `DOUDOU_SERVER_DB`，默认 `./data/traces.db` | SQLite 位置 |

## trace 记了什么

一条 trace 对应一次上游调用；工具续轮会各自记一条。

| 字段 | 含义 |
|---|---|
| `ts` / `model` / `endpoint` / `round` | 时间、模型、地址、剩余可续轮数 |
| `sys` / `tool` / `mem` / `sum` / `hist` | 上下文各层估算 token：系统提示、工具定义、记忆召回、滚动摘要、历史消息 |
| `dropped` / `truncated` / `overflow` | 被丢弃的条数、被截断的条数、是否超预算 |
| `calls` | 本轮发起的工具名 |
| `finish` / `ms` / `ok` / `err` | 结束原因、总耗时、是否成功、错误描述 |
| `request_body` | 发给上游的请求体原文（`x_doudou_ctx` 会被剔除后落库） |
| `response_raw` | 上游 SSE 原始回复（逐行拼接，不截断，上限 64MB 仅防异常无限流） |

客户端提前断开连接（`Broken pipe` / `Connection reset` 等）记为 `finish=cancel`、`ok=true`，
不算调用失败。

## 看板

根路径 `/` 是一个**单文件、零外部依赖**的页面（没有 CDN、没有构建步骤、断网可用）：

- EventSource 实时插入新 trace，可暂停；
- 详情抽屉展示完整请求体与原始回复；
- JSON / SSE token 着色（单次线性正则扫描，1MB 以内开启，超过自动关闭以保证滚动流畅）；
- 「上下文」列显示各层 token 汇总与明细。

## 设计取舍

- **不用 ORM**：单表 + `JdbcTemplate`，schema 稳定，查询就是几条 SQL。
- **不用三方 HTTP 客户端**：Java 原生 `HttpClient` + `BodyHandlers.ofInputStream()` 读上游，
  `StreamingResponseBody` 回吐，保住打字机效果。
- **没有认证**：定位是局域网内的个人可观测服务，不是对外产品。
- **Spring Boot 4 的 `starter-web` 不再自带 Jackson**，代理解析 SSE 需要显式引入 `jackson-databind`。
- **已有库加列**走 `pragma_table_info` + `ALTER TABLE`，不要求删库重建。

## 相关

配套的 Android 客户端是独立仓库，不在此项目内。

## License

[MIT](LICENSE)
