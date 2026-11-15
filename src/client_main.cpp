#include "financial_pipeline/transport/client.hpp"
#include "financial_pipeline/pipeline/data_generator.hpp"
#include <iostream>
#include <iomanip>
#include <chrono>

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 50051;
    double volume_gb = 1.0;
    uint64_t custom_records = 0;
    size_t batch_records = 15000;
    bool use_flow_control = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--gb" && i + 1 < argc) {
            volume_gb = std::stod(argv[++i]);
        } else if (arg == "--records" && i + 1 < argc) {
            custom_records = std::stoull(argv[++i]);
        } else if (arg == "--batch_records" && i + 1 < argc) {
            batch_records = std::stoul(argv[++i]);
        } else if (arg == "--flow_control") {
            use_flow_control = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: client [options]\n"
                      << "Options:\n"
                      << "  --host <ip>             Server host (default: 127.0.0.1)\n"
                      << "  --port <port>           Server port (default: 50051)\n"
                      << "  --gb <size>             Target volume in GB (default: 1.0, set >5.0 for bulk)\n"
                      << "  --records <count>       Specific number of records (overrides --gb)\n"
                      << "  --batch_records <size>  Records per streamed chunk (default: 15000)\n"
                      << "  --flow_control          Enable bidirectional streaming with windowed ACKs\n"
                      << "  --help, -h              Show this help message\n";
            return 0;
        }
    }

    std::cout << "=======================================================\n"
              << "   Financial Data Pipeline M2M Client v1.0.0          \n"
              << "=======================================================\n"
              << " Target Endpoint    : " << host << ":" << port << "\n"
              << " Target Volume      : " << (custom_records > 0 ? (std::to_string(custom_records) + " records") : (std::to_string(volume_gb) + " GB")) << "\n"
              << " Records/Chunk      : " << batch_records << "\n"
              << " Streaming Mode     : " << (use_flow_control ? "Bidirectional Flow-Control" : "Client Streaming (Unidirectional)") << "\n"
              << "=======================================================\n";

    financial_pipeline::transport::GrpcConfig config;
    config.host = host;
    config.port = port;

    financial_pipeline::transport::PipelineClient client(config);
    auto connect_status = client.connect();
    if (!connect_status.is_ok()) {
        std::cerr << "[Client ERROR] Failed to initialize channel: " << connect_status.to_string() << "\n";
        return 1;
    }

    std::cout << "[Client] Pinging server health...\n";
    auto health_res = client.check_health("client-cli", 5000);
    if (!health_res.is_ok()) {
        std::cerr << "[Client ERROR] Health check failed: " << health_res.status().to_string() << "\n";
        return 1;
    }
    std::cout << "[Client] Server is healthy (Version: " << health_res.value().version()
              << ", Uptime: " << health_res.value().uptime_seconds() << "s)\n";

    auto streamer = client.create_streamer();
    financial_pipeline::pipeline::SyntheticDataGenerator generator(1337);

    financial_pipeline::pipeline::StreamOptions opts;
    auto now_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    opts.session_id = "session-" + std::to_string(now_epoch);
    opts.batch_record_count = batch_records;
    opts.target_gigabytes = volume_gb;
    opts.target_total_records = custom_records;
    opts.use_flow_control = use_flow_control;
    opts.progress_callback = [](uint64_t chunks_sent, uint64_t bytes_sent) {
        double mb = static_cast<double>(bytes_sent) / (1024.0 * 1024.0);
        double gb = mb / 1024.0;
        std::cout << "\r[Client Stream] Transmitted: " << std::fixed << std::setprecision(2)
                  << gb << " GB (" << mb << " MB) across " << chunks_sent << " chunks..." << std::flush;
    };

    std::cout << "[Client] Initiating high-throughput stream for session: " << opts.session_id << "\n";

    financial_pipeline::pipeline::StreamResult result;
    if (use_flow_control) {
        result = streamer->stream_with_flow_control(generator, opts);
    } else {
        result = streamer->stream_unidirectional(generator, opts);
    }

    std::cout << "\n\n";
    if (!result.status.is_ok()) {
        std::cerr << "[Client ERROR] Transfer failed: " << result.status.to_string() << "\n";
        return 1;
    }

    std::cout << "=======================================================\n"
              << "   M2M Financial Pipeline Transfer Complete           \n"
              << "=======================================================\n"
              << " Status             : SUCCESS\n"
              << " Session ID         : " << result.session_id << "\n"
              << " Total Chunks Sent  : " << result.chunks_sent << "\n"
              << " Total Records Sent : " << result.records_sent << "\n"
              << " Total Data Volume  : " << std::fixed << std::setprecision(3)
              << (static_cast<double>(result.bytes_sent) / (1024.0 * 1024.0 * 1024.0)) << " GB ("
              << (static_cast<double>(result.bytes_sent) / (1024.0 * 1024.0)) << " MB)\n"
              << " Transfer Duration  : " << std::setprecision(2) << result.elapsed_seconds << " seconds\n"
              << " Wire Throughput    : " << result.throughput_mb_per_sec << " MB/s ("
              << (result.throughput_mb_per_sec * 8.0 / 1000.0) << " Gbps)\n"
              << " Record Throughput  : " << result.records_per_sec << " records/sec\n"
              << " Client CRC32C      : 0x" << std::hex << result.client_checksum << std::dec << "\n"
              << " Server CRC32C      : 0x" << std::hex << result.server_checksum << std::dec << "\n"
              << " Checksum Verified  : " << (result.checksum_matched ? "MATCHED (100% Bit-Exact)" : "FAILED") << "\n"
              << " Process Memory RSS : " << financial_pipeline::metrics::PipelineMetricsCollector::get_process_memory_mb() << " MB\n"
              << "=======================================================\n";

    return 0;
}
