# Financial Data Pipeline C++ gRPC M2M API Documentation

This document provides a comprehensive technical reference for the **Financial Data Pipeline** C++ gRPC Machine-to-Machine (M2M) API, designed for high-throughput, low-latency, memory-bounded transmission of large (>5 GB) financial datasets.

---

## Table of Contents
1. [Architecture & Layer Overview](#1-architecture--layer-overview)
2. [gRPC Service Definition (`.proto`)](#2-grpc-service-definition-proto)
3. [Domain Layer API](#3-domain-layer-api)
4. [Wire & Serialization Layer API](#4-wire--serialization-layer-api)
5. [Transport & Network Layer API](#5-transport--network-layer-api)
6. [Pipeline & Streaming Layer API](#6-pipeline--streaming-layer-api)
7. [Common Utilities & Integrity Verification](#7-common-utilities--integrity-verification)
8. [Telemetry & Metrics API](#8-telemetry--metrics-api)

---

## 1. Architecture & Layer Overview

The system strictly adheres to **Separation of Concerns (SoC)** and the **Single Responsibility Principle (SRP)**. There are **no god classes** or **god functions**; each component has a well-defined boundary:

| Layer | Namespace | Primary Responsibility |
|---|---|---|
| **Domain** | `financial_pipeline::domain` | Pure business entities (`TradeRecord`, `MarketDataBatch`), validations, and session state machines. Completely agnostic of gRPC or network transport. |
| **Wire / Anti-Corruption** | `financial_pipeline::wire` | Bridges domain models and wire protocols. Handles zero-copy binary serialization and deserialization. |
| **Transport** | `financial_pipeline::transport` | Channel configuration, socket tuning, server/client lifecycle management, and gRPC service dispatch. |
| **Pipeline** | `financial_pipeline::pipeline` | Chunking, streaming engines (`FinancialDataStreamer`, `FinancialDataReceiver`), data sinks (`IDataSink`), and synthetic generators. |
| **Common** | `financial_pipeline::common` | Core primitives, status codes (`PipelineStatus`, `Result<T>`), and hardware-optimized CRC32C hashing. |
| **Metrics** | `financial_pipeline::metrics` | Lock-free atomic telemetry collectors, latency trackers, and RSS memory samplers. |

---

## 2. gRPC Service Definition (`.proto`)

### Service: `FinancialDataPipeline`

```protobuf
service FinancialDataPipeline {
    rpc StreamFinancialData (stream FinancialDataChunk) returns (StreamSummary);
    rpc StreamWithFlowControl (stream FinancialDataChunk) returns (stream StreamAck);
    rpc QuerySessionStatus (SessionQuery) returns (SessionStatusResponse);
    rpc HealthCheck (HealthCheckRequest) returns (HealthCheckResponse);
}
```

### Methods

#### `StreamFinancialData`
- **Type**: Client-to-server streaming RPC.
- **Description**: Streams a sequence of `FinancialDataChunk` blocks (>5 GB total). Bounded to $O(1)$ memory consumption. Upon end-of-stream, the server computes and returns a `StreamSummary` containing bit-exact composite checksums, throughput, and duration.

#### `StreamWithFlowControl`
- **Type**: Bidirectional streaming RPC.
- **Description**: Implements application-level backpressure. The client sets `request_ack = true` at window boundaries (e.g., every 10 chunks), and the server replies with `StreamAck` confirming reception before the client transmits further.

#### `QuerySessionStatus`
- **Type**: Unary RPC.
- **Description**: Inquires about an active or completed transmission session using `session_id`.

#### `HealthCheck`
- **Type**: Unary RPC.
- **Description**: Service liveness and readiness probe for load balancers and orchestrators.

---

## 3. Domain Layer API

### `domain::TradeRecord`
Canonical business model representing an executed trade event.

```cpp
struct TradeRecord {
    common::TradeId trade_id{0};
    common::SequenceNumber sequence_number{0};
    std::string symbol;
    double price{0.0};
    double quantity{0.0};
    common::TradeSide side{common::TradeSide::Unspecified};
    common::TimestampNs timestamp_ns{0};
    std::string venue;
    std::string order_id;
    uint32_t execution_flags{0};

    bool operator==(const TradeRecord& other) const noexcept;
    bool operator!=(const TradeRecord& other) const noexcept;
    std::string to_string() const;
};
```

### `domain::MarketDataBatch`
Aggregate collection of `TradeRecord` items with continuous reserve and memory footprint estimation.

- `void add_record(TradeRecord record)`: Appends a trade record with move semantics.
- `void reserve(size_t capacity)`: Pre-allocates vector capacity to avoid reallocations.
- `size_t size() const noexcept`: Current number of records.
- `size_t estimated_byte_size() const noexcept`: Computes dynamic memory footprint in bytes.

### `domain::TradeRecordValidator`
Validates financial constraints.

- `static common::PipelineStatus validate(const TradeRecord& record) noexcept`: Checks positive price/quantity, valid symbol length, valid trade direction, non-zero timestamp.
- `static common::PipelineStatus validate(const MarketDataBatch& batch) noexcept`: Validates all records in a batch.
- `static common::PipelineStatus validate_sequence(expected, actual) noexcept`: Validates monotonic sequence ordering.

### `domain::TransferSession` & `SessionManager`
Thread-safe session state machine (`Initialized -> Active -> Completed/Failed/Cancelled`).

- `PipelineStatus on_chunk_received(chunk_idx, record_count, byte_count, chunk_crc)`: Validates expected sequence index, updates running record/byte counters, and chains chunk CRC into composite checksum.
- `SessionStatistics get_statistics() const`: Returns immutable snapshot of session metrics (throughput, total bytes, duration, checksum).

---

## 4. Wire & Serialization Layer API

### `wire::WireMapper`
Decouples domain types from protocol buffer representations.

```cpp
class WireMapper {
public:
    static void to_proto(const domain::TradeRecord& src, financial_pipeline::TradeRecord* dst);
    static void from_proto(const financial_pipeline::TradeRecord& src, domain::TradeRecord* dst);
    static void to_proto(const domain::MarketDataBatch& src, financial_pipeline::TradeBatch* dst);
    static void from_proto(const financial_pipeline::TradeBatch& src, domain::MarketDataBatch* dst);

    static std::string serialize_batch(const domain::MarketDataBatch& batch);
    static common::Result<domain::MarketDataBatch> deserialize_batch(std::string_view bytes);

    static financial_pipeline::Side to_proto_side(common::TradeSide side) noexcept;
    static common::TradeSide from_proto_side(financial_pipeline::Side side) noexcept;
    static financial_pipeline::TransferStatus to_proto_status(common::StatusCode code) noexcept;
    static common::StatusCode from_proto_status(financial_pipeline::TransferStatus status) noexcept;
};
```

---

## 5. Transport & Network Layer API

### `transport::GrpcConfig`
Configuration structure controlling gRPC channel parameters.

- `host`: Target IP or hostname (default: `127.0.0.1`).
- `port`: Port number (default: `50051`).
- `max_message_size_bytes`: Max incoming/outgoing gRPC message size (default: `32 MB`).
- `keepalive_time_ms`: TCP keepalive interval (default: `30,000 ms`).
- `enable_bdp_probe`: HTTP/2 Bandwidth Delay Product dynamic window tuning.

### `transport::PipelineServer`
Manages `grpc::Server` lifecycle.

- `common::PipelineStatus start()`: Binds configured port and starts listening.
- `void wait()`: Synchronously blocks until server shutdown.
- `void stop(int timeout_ms = 5000)`: Gracefully stops accepting new streams and drains in-flight RPCs within the timeout window.

### `transport::PipelineClient`
Manages connection channels and stubs.

- `common::PipelineStatus connect()`: Initializes tuned gRPC channel.
- `common::Result<HealthCheckResponse> check_health(client_id, timeout_ms)`: Probes server health.
- `common::Result<SessionStatusResponse> query_session(session_id, timeout_ms)`: Reads status of a remote session.
- `std::unique_ptr<pipeline::FinancialDataStreamer> create_streamer()`: Instantiates streaming client engine.

---

## 6. Pipeline & Streaming Layer API

### `pipeline::FinancialDataStreamer`
Client-side bulk transfer engine.

```cpp
StreamResult stream_unidirectional(SyntheticDataGenerator& generator, const StreamOptions& options);
StreamResult stream_with_flow_control(SyntheticDataGenerator& generator, const StreamOptions& options);
```

#### `StreamOptions`
- `session_id`: Unique transfer GUID.
- `batch_record_count`: Number of records packaged per chunk (e.g., 15,000 ~ 1.1 MB).
- `target_gigabytes`: Total volume target (e.g. `5.0` for >5 GB).
- `use_flow_control`: If true, activates bidirectional streaming with windowed ACKs.
- `ack_window_chunks`: Interval between requested ACKs (default: 10).
- `progress_callback`: Lambda triggered every N chunks reporting live transmission progress.

### `pipeline::FinancialDataReceiver`
Server-side stream consumer.

- `common::PipelineStatus process_chunk(const FinancialDataChunk& chunk)`:
  1. Computes CRC32C over payload bytes; compares against `chunk.chunk_checksum()`.
  2. Updates session state and checks sequence continuity.
  3. Deserializes payload to `MarketDataBatch` and dispatches to registered `IDataSink`.
  4. Records metrics. Memory is freed immediately upon exiting the function ($O(1)$ memory bound).

### Data Sinks (`pipeline::IDataSink`)
- `NullDataSink`: Zero-allocation discard sink used to benchmark network and serialization throughput without disk I/O bottlenecks.
- `MemoryDataSink`: In-memory storage for test assertions.
- `FileDataSink`: Persistent sink for offline storage.

---

## 7. Common Utilities & Integrity Verification

### `common::Crc32cCalculator`
Castagnoli CRC32 implementation (polynomial `0x82F63B78`).

```cpp
class Crc32cCalculator {
public:
    static uint32_t calculate(const void* data, size_t length) noexcept;
    static uint32_t update(uint32_t previous_crc, const void* data, size_t length) noexcept;
    static uint32_t finalize(uint32_t rolling_crc) noexcept;
    static uint32_t combine_checksums(uint32_t composite, uint32_t chunk_crc, uint64_t chunk_index) noexcept;
};
```
*Note: `combine_checksums` packs `chunk_crc` and `chunk_index` without struct alignment padding bytes to guarantee cross-architecture, bit-exact hash determinism.*

### `common::PipelineStatus` & `common::Result<T>`
Provides expressive error handling without throwing C++ exceptions across API or thread boundaries.

---

## 8. Telemetry & Metrics API

### `metrics::PipelineMetricsCollector`
Lock-free, thread-safe metrics collector.

- `void record_chunk(size_t byte_count, uint32_t record_count) noexcept`
- `void record_error() noexcept`
- `PipelineMetricsSnapshot get_snapshot() const noexcept`
- `static double get_process_memory_mb() noexcept`: Platform-specific query reading current process Resident Set Size (RSS) memory in MB (Windows `GetProcessMemoryInfo` and Linux `getrusage`).
