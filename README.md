# financial-pipeline-grpc

[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![C++17](https://img.shields.io/badge/standard-C%2B%2B17-blue.svg?style=flat-square)](https://en.wikipedia.org/wiki/C%2B%2B17)
[![gRPC](https://img.shields.io/badge/gRPC-v1.67.1-orange.svg?style=flat-square)](https://grpc.io/)
[![Build Status](https://img.shields.io/badge/tests-19%2F19%20passed-brightgreen.svg?style=flat-square)]()
[![Throughput](https://img.shields.io/badge/throughput-1.16%20Gbps%20%7C%202.18M%20rec%2Fs-success.svg?style=flat-square)]()
[![Linux](https://img.shields.io/badge/OS-Linux-FCC624.svg?style=flat-square&logo=linux&logoColor=black)]()
[![macOS](https://img.shields.io/badge/OS-macOS-000000.svg?style=flat-square&logo=apple&logoColor=white)]()
[![Windows](https://img.shields.io/badge/OS-Windows-0078D6.svg?style=flat-square&logo=windows&logoColor=white)]()
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED.svg?style=flat-square&logo=docker&logoColor=white)]()

High-throughput, memory-bounded C++ gRPC M2M pipeline API designed to stream >5 GB of financial tick and trade transaction data between distributed servers across Linux, macOS, and Windows.

The **financial-pipeline-grpc** engine implements a high-performance, asynchronous, and memory-bounded machine-to-machine (M2M) streaming architecture. Built with Modern C++17, Protocol Buffers, and gRPC, it achieves >1.16 Gbps wire throughput (>2.18 million trade records/sec) while strictly constraining memory utilization to $O(1)$ (<30 MB RSS). The system enforces end-to-end auditability and bit-exact data integrity via per-chunk CRC32C hashing, composite rolling hashes, sequence continuity checks, and bidirectional sliding-window flow control.

---

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [API](#api)
- [Architecture](#architecture)
- [Benchmarks](#benchmarks)
- [Contributing](#contributing)
- [License](#license)

---

## Background

Financial institutions, high-frequency market makers, and quantitative risk engines face unique challenges when transmitting multi-gigabyte datasets (>5 GB) between nodes:
1. **gRPC Message Limits**: gRPC defaults to a 4 MB maximum message limit (`GRPC_DEFAULT_MAX_RECV_MESSAGE_LENGTH`). Uncalibrated single-message transmissions trigger `RESOURCE_EXHAUSTED`.
2. **Memory Overhead**: In-memory buffering of 5 GB Protobuf objects leads to massive cache thrashing, GC/allocator stalls, and Out-Of-Memory (OOM) termination.
3. **Data Integrity & Non-Repudiation**: Regulatory regimes demand that every transmitted trade record be accounted for with zero tolerance for corrupted bits or dropped packets.
4. **Producer-Consumer Speed Mismatches**: Unregulated streaming can overwhelm receiving nodes, causing TCP buffer saturation and network disconnects.

**financial-pipeline-grpc** addresses these issues through an industrial-grade, zero-god-class architecture featuring chunked streaming, dual-layer hardware CRC32C validation, and dynamic sliding-window backpressure.

---

## Install

The pipeline supports native installation across **Linux**, **macOS**, and **Windows**, as well as containerized deployment via **Docker**.

### Prerequisites
- **Compiler**:
  - **Linux**: GCC 11+ or Clang 14+ with C++17 support.
  - **macOS**: Apple Clang (Xcode 14+) with C++17 support (Intel & Apple Silicon M1-M4).
  - **Windows**: MSVC v143/v145 (Visual Studio 2022/2026).
- **Build System**: [CMake](https://cmake.org/) (>= 3.22) and [Ninja](https://ninja-build.org/).
- **Package Manager**: [Conan 2.x](https://conan.io/) (`pip install conan`) OR system package manager (APT/Homebrew).

---

### Option A: Linux Installation (Ubuntu, Debian, Fedora, Arch)

#### 1. Automated Script
Run the automated installation, build, and test script:
```bash
chmod +x scripts/setup_linux.sh
./scripts/setup_linux.sh
```

#### 2. Manual Conan Build (Recommended)
```bash
# Install Conan & Ninja via pip
pip install --upgrade conan ninja

# Detect host compiler profile
conan profile detect --force

# Install dependencies (gRPC, Protobuf, GTest)
conan install . --output-folder=build -s build_type=Release --build=missing

# Configure and compile with CMake & Ninja
cmake -S . -B build -G "Ninja" -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

#### 3. Native APT / DNF Packages (Alternative)
On Ubuntu / Debian:
```bash
sudo apt-get update && sudo apt-get install -y \
    build-essential cmake ninja-build pkg-config \
    libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc libgtest-dev

cmake -S . -B build -G "Ninja" -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

---

### Option B: macOS Installation (Apple Silicon M1-M4 & Intel)

#### 1. Automated Script
Run the automated macOS setup script:
```bash
chmod +x scripts/setup_macos.sh
./scripts/setup_macos.sh
```

#### 2. Using Homebrew (Recommended for macOS)
```bash
# Install toolchain and dependencies via Homebrew
brew install cmake ninja grpc protobuf googletest pkg-config

# Export prefix path for Homebrew packages (supports both /opt/homebrew and /usr/local)
BREW_PREFIX="$(brew --prefix)"
export CMAKE_PREFIX_PATH="${BREW_PREFIX}:${BREW_PREFIX}/opt/grpc:${BREW_PREFIX}/opt/protobuf:${BREW_PREFIX}/opt/googletest"

# Configure and compile
cmake -S . -B build -G "Ninja" -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="${CMAKE_PREFIX_PATH}"
cmake --build build -j$(sysctl -n hw.ncpu)
```

#### 3. Using Conan on macOS
```bash
pip install --upgrade conan ninja
conan profile detect --force
conan install . --output-folder=build -s build_type=Release --build=missing
cmake -S . -B build -G "Ninja" -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(sysctl -n hw.ncpu)
```

---

### Option C: Windows Installation (MSVC & Ninja)

```powershell
# 1. Install Conan via pip
pip install conan

# 2. Install prebuilt dependencies via Conan
conan install . --output-folder=build -s compiler.version=194 -s compiler.cppstd=14 -s build_type=Release

# 3. Configure and compile using Developer Command Prompt (x64)
cmake -S . -B build -G "Ninja" -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

---

### Option D: Docker (Instant Test on Any OS)

Build and run the entire pipeline inside a self-contained container:
```bash
# Build the Docker image (runs all 19 unit & integration tests during build)
docker build -t financial-pipeline .

# Run the standalone M2M server container
docker run --rm -p 50051:50051 financial-pipeline
```

---

## Usage

### 1. Running the M2M Server

Start the standalone receiving server node (defaults to port `50051` with 32 MB max message threshold):

- **Linux / macOS**:
  ```bash
  ./build/financial_pipeline_server --port 50051 --max_msg_mb 32
  ```

- **Windows**:
  ```powershell
  .\build\financial_pipeline_server.exe --port 50051 --max_msg_mb 32
  ```

### 2. Running the M2M Client

Stream financial data to the server node:

- **Stream 5.0 GB of financial records (unidirectional)**:
  ```bash
  ./build/financial_pipeline_client --host 127.0.0.1 --port 50051 --gb 5.0 --batch_records 15000
  ```

- **Stream with Bidirectional Flow-Control (Sliding-Window ACKs)**:
  ```bash
  ./build/financial_pipeline_client --host 127.0.0.1 --port 50051 --gb 2.0 --flow_control
  ```

- **Stream a specific count of trade transactions**:
  ```bash
  ./build/financial_pipeline_client --records 10000000
  ```

### 3. Running Unit and Integration Tests

Execute the 19 automated tests covering unit hashing, domain validation, wire mapping, session state transitions, sequence gaps, checksum corruption, and live gRPC streaming:

- **Linux / macOS**:
  ```bash
  ./build/financial_pipeline_tests
  ```

- **Windows**:
  ```powershell
  .\build\financial_pipeline_tests.exe
  ```

- **Via CTest (All Platforms)**:
  ```bash
  ctest --test-dir build --output-on-failure
  ```

### 4. Running the Benchmark Suite

Execute the dedicated benchmarking suite testing raw checksum speed, batch serialization throughput, and end-to-end M2M streaming:

- **Quick 1.0 GB benchmark**:
  ```bash
  ./build/financial_pipeline_benchmark --gb 1.0
  ```

- **Full-scale >5.0 GB benchmark**:
  ```bash
  ./build/financial_pipeline_benchmark --gb 5.0
  ```

---

## API

Complete API technical documentation is available in [`docs/API.md`](docs/API.md).

### gRPC Service Definition (`proto/financial_pipeline.proto`)

```protobuf
service FinancialDataPipeline {
    // Unidirectional bulk client-streaming RPC (>5 GB transfer)
    rpc StreamFinancialData (stream FinancialDataChunk) returns (StreamSummary);

    // Bidirectional streaming with sliding-window backpressure
    rpc StreamWithFlowControl (stream FinancialDataChunk) returns (stream StreamAck);

    // Query active or completed session telemetry
    rpc QuerySessionStatus (SessionQuery) returns (SessionStatusResponse);

    // Node readiness and liveness probe
    rpc HealthCheck (HealthCheckRequest) returns (HealthCheckResponse);
}
```

### Core C++ Classes

- **`domain::TradeRecord`**: Canonical financial entity for executed market trades.
- **`domain::MarketDataBatch`**: Batched aggregate avoiding single-object allocation overhead.
- **`domain::TradeRecordValidator`**: Enforces positive prices, monotonic sequencing, and ticker integrity.
- **`domain::TransferSession`**: Thread-safe session tracker managing lifecycle and composite hashes.
- **`wire::WireMapper`**: Anti-corruption translation layer between Domain models and Protobuf wire types.
- **`common::Crc32cCalculator`**: Hardware-accelerated Castagnoli CRC32 hashing engine.
- **`transport::PipelineServer`**: Manages gRPC server binding, threading, and graceful shutdown.
- **`transport::PipelineClient`**: Channel pooling, socket tuning, and connection health management.
- **`pipeline::FinancialDataStreamer`**: Client chunking and streaming coordinator.
- **`pipeline::FinancialDataReceiver`**: Server streaming processor and data sink dispatcher.

---

## Architecture

Detailed design notes and diagrams are available in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

### Layered Separation of Concerns (SoC)

The architecture isolates concerns to avoid god classes or functions:

- **Domain Layer (`domain/`)**: Pure C++ domain entities, business validation, and session states. Has zero dependencies on gRPC or Protobuf.
- **Wire Layer (`wire/`)**: Dedicated adapter converting between domain objects and protobuf wire representations.
- **Transport Layer (`transport/`)**: Encapsulates socket tuning, HTTP/2 parameters, channel options, and gRPC service lifecycle.
- **Pipeline Layer (`pipeline/`)**: Handles chunking, checksum calculation, sink ingestion, and streaming coordination.
- **Common Layer (`common/`)**: Status objects (`PipelineStatus`, `Result<T>`) and CRC32C hashing.
- **Metrics Layer (`metrics/`)**: Lock-free atomic throughput and memory RSS sampling.

### Memory Boundedness ($O(1)$ Footprint)

Regardless of whether 1 GB, 5 GB, or 100 GB is transferred:
- The streaming client dispatches data in discrete batches (~1.1 MB per chunk).
- The server processes each chunk through an `IDataSink` and frees payload memory immediately.
- Result: **Memory consumption remains flat (<30 MB RSS)** throughout multi-gigabyte transfers.

---

## Benchmarks

Measured on an x86_64 host (MSVC Release build) during a **5.0 GB** streaming run:

| Metric | Result |
|---|---|
| **Transferred Data Volume** | **4.85 GB (5,204,400,000 bytes)** |
| **Total Records Transferred** | **74,565,404 trades** |
| **Total Streamed Chunks** | 4,972 chunks (~1.05 MB / chunk) |
| **Transfer Duration** | **34.10 seconds** |
| **Wire Throughput** | **145.54 MB/s (1.16 Gbps)** |
| **Record Throughput Rate** | **2,186,502 records/second** |
| **End-to-End CRC32C Verification** | **MATCHED (Bit-Exact)** |
| **Initial Memory RSS** | 11.05 MB |
| **Final Memory RSS** | 27.05 MB (Delta: 16.0 MB) |
| **Memory Complexity** | **$O(1)$ Bounded** |

---

## Contributing

PRs accepted. Please adhere to the following sustainable development practices:
1. Ensure all new code follows C++17 RAII principles with smart pointers and `const`-correctness.
2. Avoid god classes: keep classes focused on a single responsibility.
3. Verify that changes build and pass all 19 tests across Linux, macOS, and Windows via GitHub Actions CI (`.github/workflows/ci.yml`).
4. Keep the documentation in `docs/API.md`, `docs/ARCHITECTURE.md`, and `README.md` up to date.

Small note: If editing the Readme, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

---

## License

[MIT](LICENSE) © 2026 Financial Pipeline Contributors
