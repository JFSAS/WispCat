# WeChat Runtime

独立 Go 微信机器人运行时。微信协议、账号、同步和媒体由运行时维护，
NoneBot、AstrBot 或自定义程序通过 OneBot v11、Go SDK、MCP 接入。
机器人业务和模型策略在外部进程中运行。

支持私聊、群聊、文本、@、图片和文件；包含持久编号、消息历史、投递回执、
请求幂等、多账号及管理界面。当前为 `0.1.0-dev`，仅经过合成数据和本机回环
验证，未验证真实微信生产环境。不支持的消息类型和操作会明确拒绝。

## 构建和启动

需要 Go 1.25+、C 编译器、OpenSSL 3 和 SQLite 3。

```sh
# macOS 使用已有 Homebrew OpenSSL；Linux 先准备开发库和 pkg-config。
sh tools/go.sh build -o .bin/wechat-runtime ./cmd/wechat-runtime
sh tools/go.sh build -o .bin/wechat-mcp ./cmd/wechat-mcp
cp runtime.example.json runtime.local.json

# 三个令牌分别设置为至少 16 字节的独立随机值。
export WECHAT_RUNTIME_ADMIN_TOKEN='替换为管理令牌'
export WECHAT_ONEBOT_TOKEN='替换为OneBot令牌'
export WECHAT_BOT_TOKEN='替换为下游框架令牌'
./.bin/wechat-runtime check --config runtime.local.json
./.bin/wechat-runtime serve --config runtime.local.json
```

示例管理页为 `http://127.0.0.1:5099`，OneBot 为 `http://127.0.0.1:5700`。
示例账号默认 `offline`；需要连接微信时明确设置 `mode: online` 或使用管理操作。
支持最多 16 个账号，各自配置 `instance`、监听地址、令牌和下游连接。
同一状态目录只能由一个进程持有；备份时先正常停止进程，并完整保留目录和密钥。

macOS 与 Linux 使用同一套 macOS 4.1.13 兼容协议身份。原生 Windows 未适配。
Dockerfile 使用非 root 用户；状态卷挂载到 `/home/runtime`。监听地址限定回环，
Linux Docker 可按部署环境使用 host networking，单独发布端口不能访问容器回环地址。

## 接入机器人

反向 WebSocket 的 `reverse[].url` 指向下游框架；`token_env` 对应下游框架的令牌。
常用地址为 NoneBot 的 `ws://127.0.0.1:8080/onebot/v11/ws`，AstrBot 地址以自身配置为准。
`Universal` 连接同时承载动作和事件；握手包含 `X-Self-ID`、`X-Client-Role` 和 Bearer token。

HTTP 动作支持 GET、JSON POST 和表单；正向 WS 支持 `/`、`/api`、`/event`。
`/_actions` 返回动作及参数目录，`/_connections` 返回连接状态，都需要 OneBot 令牌。

```json
{"action":"send_private_msg","params":{"user_id":10001,"message":"hello","wechat_request_id":"my-request-id"},"echo":"request-1"}
```

数字 ID 是持久、按账号隔离的映射，不是微信号。`echo` 只关联响应。
重试使用调用方持久保存的 `wechat_request_id`：同键同内容返回原投递，内容冲突会失败。
只有确认回执才返回发送成功；排队、超时和未知结果不等于失败后可以直接重发。
回复丢失时，用 `wechat_get_delivery_by_request` 查询原请求。
事件消费者按 `wechat_event_id` 去重，WS 写成功不代表业务已处理。

图片／文件可使用 base64、HTTP(S) URL 或 `stream://` 引用，不接受任意本机路径。
混合发送最多拆成 20 条原生消息，媒体合计最多 16MiB，保留每条结果。
分块上传／下载单块最多 256KiB；临时引用按账号隔离，不跨进程重启保留。
联系人和群成员只表示已观察资料；引用／撤回、语音视频、完整成员列表和群管理尚未实现。

## SDK 和 MCP

`pkg/bot` 定义通道接口，`pkg/client` 提供外部 Go 客户端；`examples/echo` 是独立消费者。
SDK 支持调用、事件、上传和下载；下载校验完整字节及 SHA256，不自动重试发送。

`wechat-mcp` 默认 `docs` 模式；设置以下环境变量后可连接运行时：

| 环境变量 | 用途 |
| --- | --- |
| `WECHAT_MCP_ENDPOINT` | OneBot HTTP 地址 |
| `WECHAT_MCP_TOKEN` | OneBot 令牌 |
| `WECHAT_MCP_MODE` | `docs`、`read` 或 `write`；写动作和本地文件处理需要 `write` |
| `WECHAT_MCP_UPLOAD_ROOT` | 允许上传的本地目录 |
| `WECHAT_MCP_STREAM_DIR` | 下载文件的私有目录；默认使用进程私有临时目录 |

目录工具为 `list_actions`、`get_action`、`search_actions`、`list_categories`。
工具请求最多并行 8 个，本地文件操作串行；取消不撤销已经提交的发送或文件副作用。

## 管理接口

管理页与管理 API 使用独立管理令牌。保存和应用分离。

| 路径 | 用途 |
| --- | --- |
| `/accounts`、`/storage` | 账号和存储状态 |
| `/accounts/<name>/listener`、`/accounts/<name>/connections` | GET 查看，POST 保存，POST `<path>/apply` 应用 |
| `/listeners`、`/listeners/apply` | 查看整体监听修订，携带修订应用全部已保存监听，支持端口交换 |
| `/topology`、`/topology/apply` | 保存、应用账号成员列表；停用保留数据和编号 |
| `/management`、`/management/apply` | 管理地址和令牌引用交接；应用响应包含目标地址和操作编号 |
| `/configuration`、`/configuration/recover` | 整份期望配置保存与中断恢复，不立即激活运行状态 |

普通设置 POST 使用 `{"revision":"当前修订","plan":{...}}`。
整份配置的 plan 包含 `version: 1`、`management` 和完整 `accounts`；跨设置修改使用此入口。
`/listeners/apply` 使用 `{"revision":"当前整体修订"}`，其他应用／恢复操作使用 `{}`。
旧请求未退出时保留其存储所有权；配置恢复待完成期间阻止进一步修改。
整份持久化不等于统一热激活或失败回滚。

## 开发检查

依赖准备完成后离线运行：

```sh
sh tools/offline-checks.sh
python3 tools/check_baseline.py --source /path/to/original/rea
sh tools/mcp-offline-checks.sh
# 已准备相应 Python 环境后，按需运行：
sh tools/framework-offline-checks.sh nonebot aiocqhttp
sh tools/deployment-offline-checks.sh --media nonebot
```

测试只用合成账号与回环服务；macOS 脚本使用 sandbox-exec，Linux 需只有回环的网络命名空间。
框架依赖锁文件位于 `tests/framework`、`tests/application`，准备环境放在 `.bin/`。
执行结果和临时源码捕获写入 `.bin/`，不纳入 Git。

协议来源和允许的服务层插入记录保留在 `provenance/`；原协议、加密和原生传输字节由
基线检查保护。`internal/cdn/README.md` 是随协议导入的技术说明，其中静态分析路径属于上游仓库。
