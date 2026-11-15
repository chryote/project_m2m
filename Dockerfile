# syntax=docker/dockerfile:1
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install essential compilers and dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    ninja-build \
    pkg-config \
    libgrpc++-dev \
    libprotobuf-dev \
    protobuf-compiler \
    protobuf-compiler-grpc \
    libgtest-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Copy source repository
COPY . /workspace

# Configure, build, and run tests
RUN cmake -S . -B build -G "Ninja" -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build -j$(nproc) && \
    ./build/financial_pipeline_tests

# Production runtime stage
FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1.51 \
    libprotobuf32t64 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy compiled binaries from builder
COPY --from=builder /workspace/build/financial_pipeline_server /app/
COPY --from=builder /workspace/build/financial_pipeline_client /app/
COPY --from=builder /workspace/build/financial_pipeline_benchmark /app/
COPY --from=builder /workspace/build/financial_pipeline_tests /app/

EXPOSE 50051

ENTRYPOINT ["/app/financial_pipeline_server"]
CMD ["--port", "50051", "--max_msg_mb", "32"]
