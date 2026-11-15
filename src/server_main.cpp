#include "financial_pipeline/transport/server.hpp"
#include "financial_pipeline/transport/service_impl.hpp"
#include "financial_pipeline/pipeline/receiver.hpp"
#include "financial_pipeline/domain/session.hpp"
#include <iostream>
#include <csignal>
#include <thread>
#include <atomic>

namespace {
std::atomic<bool> g_shutdown_requested{false};
financial_pipeline::transport::PipelineServer* g_server{nullptr};

void signal_handler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        std::cout << "\n[Server] Shutdown signal received (" << signal << "). Gracefully stopping...\n";
        g_shutdown_requested.store(true);
        if (g_server) {
            g_server->stop(3000);
        }
    }
}
} // namespace

int main(int argc, char* argv[]) {
    int port = 50051;
    int max_msg_mb = 32;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--max_msg_mb" && i + 1 < argc) {
            max_msg_mb = std::stoi(argv[++i]);
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: server [options]\n"
                      << "Options:\n"
                      << "  --port <port>         Server port (default: 50051)\n"
                      << "  --max_msg_mb <size>   Max message size in MB (default: 32)\n"
                      << "  --help, -h            Show this help message\n";
            return 0;
        }
    }

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "=======================================================\n"
              << "   Financial Data Pipeline M2M Server v1.0.0          \n"
              << "=======================================================\n"
              << " Listening Port     : " << port << "\n"
              << " Max Message Size   : " << max_msg_mb << " MB\n"
              << " Flow Control Window: 10 chunks\n"
              << " Memory Bound Policy: O(1) Chunk Streaming\n"
              << "=======================================================\n";

    financial_pipeline::transport::GrpcConfig config;
    config.port = port;
    config.max_message_size_bytes = max_msg_mb * 1024 * 1024;

    auto session_mgr = std::make_shared<financial_pipeline::domain::SessionManager>();
    auto sink = std::make_shared<financial_pipeline::pipeline::NullDataSink>();
    auto metrics = std::make_shared<financial_pipeline::metrics::PipelineMetricsCollector>();
    auto receiver = std::make_shared<financial_pipeline::pipeline::FinancialDataReceiver>(session_mgr, sink, metrics);
    auto service = std::make_shared<financial_pipeline::transport::FinancialPipelineServiceImpl>(receiver, session_mgr);

    financial_pipeline::transport::PipelineServer server(config, service);
    g_server = &server;

    auto status = server.start();
    if (!status.is_ok()) {
        std::cerr << "[Server ERROR] " << status.to_string() << "\n";
        return 1;
    }

    std::cout << "[Server] Successfully bound and listening on " << config.address() << "\n"
              << "[Server] Ready for incoming M2M bulk financial streams. Press Ctrl+C to terminate.\n";

    server.wait();
    std::cout << "[Server] Stopped.\n";
    return 0;
}
