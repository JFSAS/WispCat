# WeChat Runtime

独立 Go 微信机器人运行时。协议和账号由运行时维护，外部程序通过 OneBot 接入。

```sh
sh tools/go.sh build -o .bin/wechat-runtime ./cmd/wechat-runtime
cp runtime.example.json runtime.local.json
./.bin/wechat-runtime serve --config runtime.local.json
```

依赖 Go、C 编译器、OpenSSL 和 SQLite。配置中的令牌使用环境变量提供。
开发测试使用合成账号和回环网络；生成物放在已忽略的 `.bin/` 中。
