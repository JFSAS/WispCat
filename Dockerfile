FROM golang:1.25-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends libssl-dev libsqlite3-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -trimpath -o /out/wechat-runtime ./cmd/wechat-runtime

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 libsqlite3-0 ca-certificates && rm -rf /var/lib/apt/lists/* && useradd -m -u 10001 runtime
COPY --from=build /out/wechat-runtime /usr/local/bin/wechat-runtime
USER runtime
WORKDIR /home/runtime
ENTRYPOINT ["wechat-runtime"]
CMD ["serve", "--config", "/config/runtime.json"]
