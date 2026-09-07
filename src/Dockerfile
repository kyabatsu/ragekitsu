# One dependency, so this stays a two-stage-free, single-layer image.
#
# node:22-alpine and not 20: the whole database layer is `node:sqlite`, which
# only exists from 22.5 and only stops needing --experimental-sqlite around
# 22.13. Pinning the major here rather than tracking `latest` means a DSM
# rebuild months from now produces the same runtime.
#
# ffprobe comes in because scripts/probe-media.js needs it — the durations,
# codecs and local clocks it writes are what the theater scales its timeline
# to. It is a one-off you run by hand, but wanting it and not having it means
# rebuilding the image at the worst moment.
FROM node:22-alpine AS whisper
RUN apk add --no-cache build-base cmake git
RUN git clone --depth 1 --branch v1.9.3 https://github.com/ggml-org/whisper.cpp /w
WORKDIR /w
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF \
      -DWHISPER_BUILD_TESTS=OFF -DWHISPER_BUILD_SERVER=OFF \
 && cmake --build build -j2 --config Release

FROM node:22-alpine

RUN apk add --no-cache ffmpeg tini libstdc++ libgomp
COPY --from=whisper /w/build/bin/whisper-cli /usr/local/bin/whisper-cli
WORKDIR /app

# Copied first and separately so `npm ci` is cached against dependency changes
# rather than every edit to server.js.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY . .

# Nothing in here writes to /app. The database lives in /data and the media is
# mounted read-only, so the image can run unprivileged.
USER node

ENV NODE_ENV=production \
    PORT=8000 \
    TENMA_DB=/data/archive.db \
    TENMA_MEDIA_ROOT=/media \
    TENMA_CACHE_ROOT=/cache \
    TENMA_HOST=0.0.0.0


EXPOSE 8000

# tini, because node as PID 1 does not reap children or forward SIGTERM
# properly, and an unclean stop on a WAL-mode SQLite database is the one
# shutdown you do not want to get wrong.
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "server.js"]
