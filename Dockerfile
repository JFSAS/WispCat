FROM node:24-bookworm-slim AS webui
WORKDIR /build/webui
COPY webui/package.json webui/package-lock.json ./
RUN npm ci
COPY webui/ ./
RUN npm run build

FROM golang:1.25-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends libssl-dev libsqlite3-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=webui /build/internal/host/web/dist ./internal/host/web/dist
RUN CGO_ENABLED=1 go build -trimpath -o /out/wispcat ./cmd/wispcat

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 libsqlite3-0 ca-certificates && rm -rf /var/lib/apt/lists/* && useradd -m -u 10001 runtime
COPY --from=build /out/wispcat /usr/local/bin/wispcat
USER runtime
WORKDIR /home/runtime
ENTRYPOINT ["wispcat"]
CMD ["serve", "--config", "/config/runtime.json"]
