# syntax=docker/dockerfile:1

# Build on the native platform and cross-compile, so multi-arch builds
# (docker buildx build --platform linux/amd64,linux/arm64) don't need emulation.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/download-craft .
# Writable by any user, like /tmp, so the container can run as the owner of
# a bind-mounted folder (--user "$(id -u):$(id -g)").
RUN mkdir -p /rootfs/downloads /rootfs/config && chmod 1777 /rootfs/downloads /rootfs/config

# Static binary plus CA certificates for HTTPS; runs as a non-root user.
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /rootfs/ /
COPY --from=build /out/download-craft /usr/local/bin/download-craft
WORKDIR /downloads
VOLUME ["/downloads", "/config"]
EXPOSE 7878
ENTRYPOINT ["download-craft"]
# Listen on all interfaces so a published port can reach the UI, but still
# only accept requests addressed to localhost: publish it on 127.0.0.1.
CMD ["ui", "-addr", ":7878", "-localhost", "-d", "/downloads", "-list", "/config/downloads.json"]
