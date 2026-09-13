FROM debian:bookworm-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -S . -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_TESTING=OFF \
    && cmake --build build --parallel

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder --chown=65532:65532 \
    /src/build/diskeyv-server /usr/local/bin/diskeyv-server
COPY --from=builder --chown=65532:65532 \
    /src/build/diskeyv-client /usr/local/bin/diskeyv-client

USER 65532:65532
EXPOSE 5000

HEALTHCHECK --interval=2s --timeout=2s --start-period=2s --retries=10 \
    CMD ["/usr/local/bin/diskeyv-client", "health", "5000"]

ENTRYPOINT ["/usr/local/bin/diskeyv-server"]
