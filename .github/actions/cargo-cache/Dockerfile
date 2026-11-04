# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

FROM busybox:1.37.0-musl@sha256:5cec3fc171c87218698e85a52af7087de727372aae264a787b8112901a5b0092 AS restore
ARG CACHE_STAMP
RUN --mount=type=bind,target=/cache \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/backend/target \
    test -n "$CACHE_STAMP" \
    && tar -xzf /cache/registry.tar.gz -C /usr/local/cargo/registry \
    && tar -xzf /cache/git.tar.gz -C /usr/local/cargo/git \
    && tar -xzf /cache/target.tar.gz -C /app/backend/target

FROM busybox:1.37.0-musl@sha256:5cec3fc171c87218698e85a52af7087de727372aae264a787b8112901a5b0092 AS snapshot
ARG CACHE_STAMP
# 直接压缩 mount 内容，构建层和 runner 只保存归档，避免多份展开的编译产物占满磁盘
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/backend/target \
    test -n "$CACHE_STAMP" \
    && mkdir -p /cache \
    && tar -czf /cache/registry.tar.gz -C /usr/local/cargo/registry . \
    && tar -czf /cache/git.tar.gz -C /usr/local/cargo/git . \
    && tar -czf /cache/target.tar.gz -C /app/backend/target .
