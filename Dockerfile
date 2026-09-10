# Build the headless git server. Multi-arch via buildx (TARGETOS/TARGETARCH).
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /forged ./cmd/forged

FROM alpine:3.21
# forged shells out to real git for repack and the fork fallbacks; ca-certs for
# S3/TLS egress.
RUN apk add --no-cache git ca-certificates && adduser -D -u 10001 forge
COPY --from=build /forged /usr/local/bin/forged
USER forge
# Bind all interfaces inside the container (the default is localhost-only).
ENV FORGE_ADDR=0.0.0.0:8347 \
    FORGE_DATA_DIR=/data
VOLUME /data
EXPOSE 8347
ENTRYPOINT ["forged"]
CMD ["serve"]
