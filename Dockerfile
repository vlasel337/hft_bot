ARG GO_VERSION=1
FROM golang:${GO_VERSION}-bookworm as builder

WORKDIR /usr/src/app
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN go build -v -o /run-app .


FROM debian:bookworm

# Устанавливаем корневые сертификаты для HTTPS и данные временных зон
# Устанавливаем переменные для неинтерактивной установки tzdata
ENV TZ=Etc/UTC DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates tzdata && \
    # Очищаем кэш apt для уменьшения размера образа
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /run-app /usr/local/bin/
CMD ["run-app"]
