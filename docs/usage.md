# WispCat 使用指南

[返回 README](../README.md) · [架构图](../assets/readme/architecture.svg) · [下载发行包](https://github.com/JFSAS/WispCat/releases)

本文按当前 `main` 编写。最近公开的可执行发行包为 `v0.1.0-alpha.2`；
有限故障恢复、细分错误日志等后续修复已经进入 `main`，使用旧包时请同时查看该版 Release 说明。
需要当前源码行为时，使用[源码构建](#从源码构建)。

## 目录

- [先确定部署方式](#先确定部署方式)
- [下载、配置与启动](#下载配置与启动)
- [登录微信](#登录微信)
- [接入 AstrBot](#接入-astrbot)
- [接入 NoneBot](#接入-nonebot)
- [接入自定义程序与 MCP](#接入自定义程序与-mcp)
- [管理页面的保存与应用](#管理页面的保存与应用)
- [多账号、备份与升级](#多账号备份与升级)
- [常见问题与排查顺序](#常见问题与排查顺序)
- [从源码构建](#从源码构建)

## 先确定部署方式

WispCat 负责微信账号、登录、同步、媒体和 OneBot 接口。AstrBot、NoneBot 或你的程序负责
决定是否回复、调用什么模型、使用哪个角色，以及允许执行哪些工具。
启动 WispCat 后，还需要接入并配置下游框架，才能获得 AI 回复。

| 部署方式 | 凭证准备 | 下游连接 |
| --- | --- | --- |
| 全部运行在 Mac | 使用本机导出的 `.wxlogin` 文件，或在管理页选择二维码登录 | 框架与 WispCat 通过本机回环地址连接 |
| Mac 准备凭证，Linux 运行机器人 | Mac 导出加密文件，通过转发后的管理页上传到 Linux | WispCat 和框架可共同运行在 Linux |
| 多台机器分别运行协议端与框架 | 凭证只导入运行 WispCat 的机器 | 单独安排受控网络或 SSH 转发，并保证双方令牌一致 |

以下示例默认 WispCat 和机器人框架位于同一台机器。管理端口为 `5099`，
账号 `main` 的 OneBot 端口为 `5700`；AstrBot 反向 WebSocket 端口为 `6199`。
这些端口承担不同职责：

| 地址 | 用途 | 使用的凭据 |
| --- | --- | --- |
| `http://127.0.0.1:5099/` | WispCat 管理页面与管理 API | `WISPCAT_ADMIN_TOKEN` |
| `http://127.0.0.1:5700` | OneBot 动作、正向 WebSocket、媒体获取 | `WISPCAT_ONEBOT_TOKEN` |
| `ws://127.0.0.1:6199/ws` | WispCat 主动连接 AstrBot | `WISPCAT_ASTRBOT_TOKEN`，与 AstrBot 中的反向 Token 一致 |
| `ws://127.0.0.1:8080/onebot/v11/ws` | WispCat 主动连接 NoneBot | `WISPCAT_BOT_TOKEN`，与 NoneBot 的访问令牌一致 |

“管理令牌”相当于控制台密码；`.wxlogin` 是加密的微信登录材料；
“导出密码”用于解密这个文件；模型 API Key 则填写在下游框架中。它们不能互相替代。

## 下载、配置与启动

### 1. 下载并解压

在 [Releases](https://github.com/JFSAS/WispCat/releases) 选择匹配系统和 CPU 的包：
Apple Silicon 对应 `darwin_arm64`，Intel Mac 对应 `darwin_amd64`，
普通 x86 Linux 对应 `linux_amd64`，ARM Linux 对应 `linux_arm64`。
macOS 包要求 macOS 15+，Linux 包要求 glibc 2.35+；原生 Windows 暂未提供。

下载同版 `SHA256SUMS`，在下载目录验证。例如 Apple Silicon：

```sh
grep 'wispcat_v0.1.0-alpha.2_darwin_arm64.tar.gz$' SHA256SUMS | shasum -a 256 -c -
tar -xzf wispcat_v0.1.0-alpha.2_darwin_arm64.tar.gz
cd wispcat_v0.1.0-alpha.2_darwin_arm64
```

Linux 将文件名换为对应架构，并将校验命令换为 `sha256sum -c -`。
保持可执行文件与 `lib/` 的相对位置，运行时无需安装 Go、Node.js 或开发库。
macOS 包采用 ad-hoc 签名，尚未 Apple 公证；首次运行按系统提示确认。

### 2. 保存启动令牌

在解压后的目录生成一个私有环境文件。下面的命令只在文件不存在时生成令牌，
之后每次启动都加载同一份，避免重新生成令牌后与框架配置不一致。

```sh
if [ ! -f .env.local ]; then
  (
    umask 077
    for name in WISPCAT_ADMIN_TOKEN WISPCAT_ONEBOT_TOKEN WISPCAT_BOT_TOKEN WISPCAT_ASTRBOT_TOKEN; do
      printf 'export %s=%s\n' "$name" "$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')"
    done > .env.local
  )
fi
. ./.env.local
```

令牌值保存在 `.env.local`，不要提交到公开仓库。
把对应的下游令牌复制到框架的私有设置中；管理页面的“令牌环境变量”填写变量名，
例如 `WISPCAT_ASTRBOT_TOKEN`，而非令牌值。

修改环境文件不会改变已经运行的进程环境。新增或更换令牌后，需要正常停止 WispCat，
重新加载环境文件再启动，然后同步下游配置。

### 3. 准备配置

```sh
cp wispcat.example.json wispcat.local.json
```

初次运行可保留示例：账号名为 `main`，实例为 `instance:main`，以 `offline` 启动，
下游预置为 NoneBot。你可以先进入管理页，再修改机器人连接。

配置字段的含义如下：

| 字段 | 示例 | 含义 |
| --- | --- | --- |
| `state_dir` | `.private-state` | 加密状态、密钥和持久化配置目录；相对路径以进程工作目录为基准 |
| `management_listen` | `127.0.0.1:5099` | 管理页面与 API 的监听地址，仅接受回环地址 |
| `management_token_env` | `WISPCAT_ADMIN_TOKEN` | 管理令牌的环境变量名 |
| `accounts[].name` | `main` | 管理页面中的账号名；不是微信号 |
| `accounts[].instance` | `instance:main` | 稳定的账号实例标识，用于隔离数据 |
| `accounts[].mode` | `offline` / `online` | 启动时仅查看缓存，或尝试用缓存连接微信 |
| `accounts[].listen` | `127.0.0.1:5700` | 该账号的 OneBot 监听地址 |
| `accounts[].token_env` | `WISPCAT_ONEBOT_TOKEN` | 该账号的 OneBot 访问令牌变量名 |
| `reverse[].url` | `ws://127.0.0.1:6199/ws` | 框架提供的反向 WebSocket 地址 |
| `reverse[].token_env` | `WISPCAT_ASTRBOT_TOKEN` | 反向 WebSocket 的令牌变量名 |
| `reverse[].role` | `Universal` | 同时承载动作与事件；也支持单独的 `API`、`Event` |

如果希望重启后自动使用已有缓存连接，可在**首次初始化状态目录前**把账号 `mode` 设为
`online`。没有可用缓存时会等待你选择登录方式，不会自行扫码或导入磁盘材料。
保留 `offline` 的账号重启后仍为离线，可在管理页手动“恢复连接”。

管理页保存过设置后，状态目录中的持久化配置具有优先级。只修改引导配置文件，
不一定会改变已经保存的账号、监听与连接；不要通过删除状态文件来强制覆盖。

### 4. 校验并运行

```sh
./wispcat check --config wispcat.local.json
./wispcat serve --config wispcat.local.json
```

`check` 应输出 `configuration valid; no runtime started`。它只校验配置，不会启动账号或登录微信。
正常启动后会输出管理地址和账号数量。打开 `http://127.0.0.1:5099/`，
输入 `.env.local` 中的 `WISPCAT_ADMIN_TOKEN`。

后续启动始终在同一个工作目录执行：

```sh
. ./.env.local
./wispcat serve --config wispcat.local.json
```

前台运行时，用 `Ctrl+C` 正常停止并等待退出。使用进程管理器时，也要让启动脚本加载
同一份私有环境文件，并固定工作目录；同一状态目录只能由一个进程持有。

## 登录微信

### Mac 导出加密文件

凭证提取工具支持已核实的微信 **4.1.13（269602）**。先让对应的官方客户端正常登录，
再在 Mac 的发行包目录中用默认 zsh 执行：

```zsh
read -rs 'WISPCAT_CREDENTIAL_PASSPHRASE?导出密码（至少16字节）：'
echo
export WISPCAT_CREDENTIAL_PASSPHRASE
./wispcat-credentials --app /Applications/WeChat.app --out ~/Desktop/wispcat-login.wxlogin
unset WISPCAT_CREDENTIAL_PASSPHRASE
```

把 `--app` 改为实际安装路径；例如安装名为 `微信.app` 时，使用 `/Applications/微信.app`。
`--out` 目标必须不存在，工具会拒绝覆盖。导出成功只表示生成了加密文件，
微信服务器是否接受其中票据，要在导入后的鉴权阶段确认。

### 管理页上传登录

1. 在 WispCat 管理页选择目标账号，进入“登录管理”。
2. 选择“凭证文件登录”，上传 `.wxlogin`，输入刚才的导出密码。
3. 点击“上传凭证登录”，等待鉴权与首次同步。
4. 确认账号显示“已连接”、会话显示“已验证”，最近同步时间持续更新。

上传接受、HTTP 202 或文件解密成功，都不等于微信已经在线。
首次成功后，运行时使用自己的加密缓存恢复；正常重启无需每次重新导出文件。

### Mac 访问 Linux 管理页

先在 Linux 启动 WispCat，再在 Mac 另开终端建立转发：

```sh
ssh -N -L 127.0.0.1:5099:127.0.0.1:5099 user@linux-host
```

保持转发终端运行，在 Mac 打开 `http://127.0.0.1:5099/`，上传本机文件即可。
管理接口会核对 Host；这个示例的本地和远端端口均为 `5099`，浏览器也使用 `127.0.0.1`。
如果本地端口被占用，先处理端口冲突，或在管理设置中规划一组两端一致的新端口。

如需同时访问远端 AstrBot，可再转发它实际使用的管理端口，例如：

```sh
ssh -N \
  -L 127.0.0.1:5099:127.0.0.1:5099 \
  -L 127.0.0.1:6185:127.0.0.1:6185 \
  user@linux-host
```

### 二维码与缓存恢复

管理页也提供“二维码登录”：申请二维码后，用对应微信账号扫码并确认，然后等待首次同步。
缓存恢复、申请新二维码和导入新凭证是独立操作；故障时先看错误类别，避免连续重新登录。
二维码流程的实际可用性受客户端与服务器变化影响，登录失败时保留诊断。

## 接入 AstrBot

AstrBot 独立安装和运行，参考其[官方安装文档](https://docs.astrbot.app/)。
以下按 [OneBot v11 官方接入说明](https://docs.astrbot.app/platform/aiocqhttp.html)与已验证的 4.29.0 配置编写。

### 1. 在 AstrBot 创建 OneBot 平台

在 WebUI 的机器人／平台管理中创建 **OneBot v11（aiocqhttp）** 实例：

| 设置 | 同机部署示例 |
| --- | --- |
| ID | `wechat-onebot`，用来区分平台实例 |
| 启用 | 开启 |
| 反向 WebSocket 主机 | `127.0.0.1` |
| 反向 WebSocket 端口 | `6199` |
| 反向 WebSocket Token | WispCat 启动环境中 `WISPCAT_ASTRBOT_TOKEN` 的**值** |

在这个连接中，AstrBot 提供 WebSocket 服务端，WispCat 是主动连接它的客户端。
跨容器或跨机器时，`127.0.0.1` 指向各自所在的网络环境，需另外安排可达地址。

### 2. 在 WispCat 配置反向连接

1. 进入“机器人连接”，选择账号 `main`。
2. 添加连接，选择 **AstrBot** 预设。
3. 核对地址 `ws://127.0.0.1:6199/ws`、令牌环境变量 `WISPCAT_ASTRBOT_TOKEN`、角色 `Universal`。
4. 点击“保存连接”，再点击“应用连接配置”。
5. 确认连接状态变为 `connected`；AstrBot 日志应出现 OneBot 适配器已连接。

如默认的 NoneBot 连接不再使用，可在页面中移除并应用。让两个自动回复框架同时消费
同一账号时，各框架都可能作出回复；初次联调建议只启用一个。

### 3. 配置模型、角色、权限与群聊

在 AstrBot 中配置模型提供商、API 地址、Key、模型名及角色。
微信用户的数字 ID 是 WispCat 的映射编号，不是微信号；可私聊机器人发送 `/sid`，
将返回的 ID 配置为 AstrBot 管理员。管理权限与工具执行沙箱由 AstrBot 配置，
WispCat 的管理令牌不提供聊天侧管理员身份。

群聊使用原生流程，无需安装 WispCat 补丁：

| AstrBot 设置 | 建议值 / 含义 |
| --- | --- |
| `provider_ltm_settings.group_icl_enable` | `true`，启用群聊背景 |
| `provider_ltm_settings.group_message_max_cnt` | `1000`，原生记录上限 |
| `platform_settings.ignore_at_all` | `true`，忽略 @ 全体成员的唤醒 |
| `provider_ltm_settings.active_reply.enable` | 不希望主动插话时设为 `false` |
| `provider_ltm_settings.image_caption` | 需要历史图片背景时开启 |
| `provider_ltm_settings.image_caption_provider_id` | 填写已配置、支持图片的提供商 ID |

原生图片背景会在图片到达时调用视觉模型生成描述，后续请求使用这段文字描述。
模型名或声明支持图片并不会自动开启这个功能，描述提供商也要填写。
单独发送一个 @ 默认等待后续内容；测试历史图片时，先发图，再发送“@机器人，这张图是什么”。
原生规则也允许引用机器人消息、唤醒前缀和插件命令触发，背景在模型请求时消费，
这版背景缓存保存在内存中，重启后重新积累。

## 接入 NoneBot

已有 NoneBot 项目可直接注册 OneBot v11 适配器，并沿用自己的插件。
下面是在一个独立目录建立最小示例的方式，基于
[NoneBot 手动建项说明](https://nonebot.dev/docs/2.4.4/tutorial/application)和
[OneBot 适配器安装说明](https://onebot.adapters.nonebot.dev/docs/guide/installation/)。

```sh
mkdir wispcat-nonebot
cd wispcat-nonebot
python3 -m venv .venv
. .venv/bin/activate
python -m pip install 'nonebot2[fastapi]' nonebot-adapter-onebot
```

创建 `bot.py`：

```python
import os
import nonebot
from nonebot.adapters.onebot.v11 import Adapter

nonebot.init(
    driver="~fastapi",
    host="127.0.0.1",
    port=8080,
    onebot_access_token=os.environ["WISPCAT_BOT_TOKEN"],
)
nonebot.get_driver().register_adapter(Adapter)
nonebot.load_builtin_plugins("echo")

if __name__ == "__main__":
    nonebot.run()
```

在 NoneBot 的启动终端中加载 WispCat 那份 `.env.local`（使用实际路径），然后执行
`python bot.py`。WispCat 连接选择 **NoneBot** 预设：地址为
`ws://127.0.0.1:8080/onebot/v11/ws`，令牌变量为 `WISPCAT_BOT_TOKEN`，角色为 `Universal`。
保存并应用，确认连接成功后，私聊发送 `/echo WispCat` 验证回复。
模型和业务插件需在 NoneBot 中另行配置。

## 接入自定义程序与 MCP

### 先用只读接口确认连通

在已加载 `.env.local`、能够访问对应端口的终端中执行：

```sh
curl --fail --silent --show-error \
  -H "Authorization: Bearer $WISPCAT_ONEBOT_TOKEN" \
  http://127.0.0.1:5700/get_status

curl --fail --silent --show-error \
  -H "Authorization: Bearer $WISPCAT_ONEBOT_TOKEN" \
  http://127.0.0.1:5700/_connections
```

微信侧可用时，`get_status` 返回的 `data.online`、`data.good` 应为 `true`。
`/_connections` 用于检查下游连接，`/_actions` 可查看该运行时支持的动作。
“微信在线”和“框架已连接”是两项独立检查，两者都正常才能完成回复链路。

### Go 消费者

在源码目录构建并运行[独立消费者示例](../examples/echo/main.go)：

```sh
sh tools/go.sh build -o .bin/echo ./examples/echo
export WISPCAT_ONEBOT_ENDPOINT=http://127.0.0.1:5700
. ./.env.local
./.bin/echo
```

该示例收到 `/ping` 后发送 `pong`，会产生真实回复。
自行实现发送时，为一次业务请求生成并持久保存 `wechat_request_id`，不确定结果时先用
`wechat_get_delivery_by_request` 查询；不要换一个新请求 ID 盲目重发。

### stdio MCP

将发行包中的 `wispcat-mcp` 配置为 MCP 客户端启动的程序。
先使用 `docs` 模式浏览动作，再按需要选择 `read` 或显式的 `write`。
一个只读配置示例：

```json
{
  "mcpServers": {
    "wispcat": {
      "command": "/absolute/path/to/wispcat-mcp",
      "env": {
        "WISPCAT_MCP_MODE": "read",
        "WISPCAT_MCP_ENDPOINT": "http://127.0.0.1:5700",
        "WISPCAT_MCP_TOKEN": "replace-with-your-onebot-token"
      }
    }
  }
}
```

客户端配置位置因客户端而异；在私有配置中替换可执行路径与实际 OneBot 令牌。
`write` 模式还支持设置 `WISPCAT_MCP_UPLOAD_ROOT`、`WISPCAT_MCP_STREAM_DIR`，
用于限制本地文件操作范围。取消调用不能撤销已经提交的发送。

## 管理页面的保存与应用

| 页面 / 操作 | 生效方式 |
| --- | --- |
| 添加或停用账号 | 保存账号列表，再“应用账号列表” |
| 修改 OneBot 监听 | 保存设置，再“应用已保存设置” |
| 添加、编辑、移除机器人连接 | 保存连接，再“应用连接配置” |
| 上传登录文件、申请二维码、恢复连接 | 执行独立账号操作，等待实际状态结果 |

“保存”记录期望配置，“应用”改变运行中的服务；看到“等待应用”时，原有配置可能仍在工作。
连接修改可以单独应用，微信会话继续运行。改变监听或管理端口时，后续访问使用新地址。

新增账号的令牌环境变量必须已存在于 WispCat 进程的启动环境中。
页面只保存变量名，不会替你在操作系统中创建变量或为现有进程注入新的环境。

## 多账号、备份与升级

### 添加另一个账号

每个账号使用不同的名称、实例标识、OneBot 端口和令牌变量。例如：

| 账号 | 实例 | OneBot 端口 | 令牌变量 |
| --- | --- | --- | --- |
| `main` | `instance:main` | `5700` | `WISPCAT_ONEBOT_TOKEN` |
| `work` | `instance:work` | `5701` | `WISPCAT_WORK_TOKEN` |

先在启动环境中准备新变量并重启 WispCat，再到“账号管理”添加账号、应用列表，
选择该账号导入对应凭证并配置它的下游连接。新账号默认离线。
不同微信账号不要共用一个实例；同一账号的重新登录也不要随意换实例，否则会得到新的缓存命名空间。

### 备份状态

先正常停止 WispCat，再备份完整状态目录、启动配置与私有环境文件。使用默认目录时：

```sh
umask 077
wispcat_backup_dir="$HOME/wispcat-backups"
mkdir -p "$wispcat_backup_dir"
tar -czf "$wispcat_backup_dir/wispcat-$(date +%Y%m%d-%H%M%S).tar.gz" \
  .private-state wispcat.local.json .env.local
```

如果设置过其他 `state_dir`，替换为实际路径。
密钥与加密数据需要一起恢复；只复制数据库或检查点、遗漏 `.key` 文件，会导致无法解密。
备份包含凭证，应保存在私有位置；`~/.local/state/astrbot` 等下游状态需按自己的部署位置另行备份。

### 升级程序

1. 正常停止 WispCat，完成上述备份。
2. 下载同系统、同架构的新包并校验，解压到单独目录。
3. 在原工作目录更新可执行文件及其配套 `lib/`，保留原来的配置、环境文件和状态目录。
4. 加载原环境文件，运行 `check`，再 `serve`。
5. 检查微信状态、最近同步时间和下游连接，然后验证一条消息。

更新源码不代表正在运行的程序已经升级；源码构建用户需重新构建并正常重启。
Linux 容器用户还需持久挂载状态目录、确认读写权限，并考虑回环监听的网络边界。
仓库 Dockerfile 使用非 root 用户；端口映射本身不能把容器的回环服务变成对外监听。

## 常见问题与排查顺序

按“进程与端口 → 微信状态 → 下游连接 → 模型／插件”顺序检查，先区分断在哪一层。

| 现象 | 优先检查 |
| --- | --- |
| `token_env_unavailable_or_invalid` | 启动终端是否加载环境文件；变量名是否与配置一致；令牌至少 16 字节 |
| 管理页提示认证失败 | 使用的是管理令牌，而非 OneBot 令牌或导出密码 |
| `host_not_allowed` / 403 | 浏览器使用的主机名、端口及 SSH 转发是否与管理监听一致 |
| 端口已占用 | 是否启动了第二个进程；管理、账号、框架三类端口是否重复 |
| 状态目录被占用 | 同一目录只允许一个运行时；先正常停止旧进程，不要强删锁文件 |
| 上传接受但没有在线 | 查看“登录管理”的鉴权结果与最近同步；检查导出版本、密码、文件和票据状态 |
| 重启后是离线 | 查看账号持久化的启动模式；示例默认为 `offline`，可先手动恢复连接 |
| 微信在线但框架未连接 | 框架是否启动；地址路径、网络环境、令牌是否匹配；修改是否已应用 |
| AstrBot 提示没有模型 | 在 AstrBot 配置并选择模型提供商，模型不在 WispCat 中配置 |
| 私聊能回复，群里没有反应 | 检查框架唤醒条件、群配置；单独 @ 可能进入原生等待后续内容流程 |
| 先发图片、再 @ 时无法理解图片 | 开启原生群背景与图片描述，选择视觉提供商；切换配置后用新图片验证 |
| 只有“[图片]”文字 | 图片摘要不是图像输入；核对下游是否实际取图或生成了图片描述 |
| 程序还活着但不再同步 | 检查 `online`、`wechat_state`、最近同步、错误类别和日志，而非只看进程存在 |
| 存储暂停 / 保存错误 | 检查磁盘空间、权限、状态目录可用性；保留原密钥和缓存，修复后等待下一次本地保存探测自动续跑 |

当前 `main` 的日志事件名为 `wechat_runtime_failure`，包含错误类别、操作阶段、原因、重试次数
与上限。管理状态的 `connection.last_error` 提供最近一次诊断。网络最多追加 6 次重试，
已验证会话的同步格式异常最多追加 2 次。保存先追加 3 次快速重试，然后进入“存储暂停”，
每 60 秒仅重试当前批次的本地保存，期间不进行新的同步、发送或登录；
保存成功后自动继续原会话。可以在 `connection.last_error.next_retry_at` 查看下次探测时间，
手动离线和停止进程均可立即中断等待。
每轮自动恢复最多发起 3 次鉴权请求；存储探测不会重置登录预算，
网络或协议重试耗尽仍等待明确操作，不能用不断重新登录来代替排查。

## 从源码构建

需要 Go 1.25+、C 编译器、OpenSSL 3 和 SQLite 3 开发库。Go 安装方式见
[官方说明](https://go.dev/doc/install)。macOS 使用 Xcode 命令行工具与 Homebrew `openssl@3`；
Linux 的 Debian／Ubuntu 环境可准备以下原生依赖：

```sh
sudo apt-get update
sudo apt-get install -y gcc pkg-config libssl-dev libsqlite3-dev
```

然后获取源码并构建：

```sh
git clone https://github.com/JFSAS/WispCat.git
cd WispCat
sh tools/go.sh build -trimpath -o .bin/wispcat ./cmd/wispcat
sh tools/go.sh build -trimpath -o .bin/wispcat-mcp ./cmd/wispcat-mcp
# 在 Mac 上准备凭证提取工具
sh tools/go.sh build -trimpath -o .bin/wispcat-credentials ./cmd/wispcat-credentials
```

源码构建需要安装原生库；上文“发行包无需开发库”不适用于源码编译。
仓库已提交前端产物，无需先安装 Node.js；修改前端时才需要在 `webui/` 执行
`npm ci`、`npm run build`，然后重新构建 Go 程序。
按照前文准备配置和环境，将命令中的 `./wispcat` 换为 `./.bin/wispcat` 即可。

使用前请阅读 [授权与免责声明](../README.md#授权与免责声明)。
