#!/usr/bin/env bash
set -euo pipefail

echo "=========================================================="
echo "  Setting up Financial Pipeline on Linux (Ubuntu/Debian/RHEL)"
echo "=========================================================="

BUILD_MODE="${1:-conan}" # Options: "conan" (recommended) or "system"

if [[ "$BUILD_MODE" == "system" ]]; then
    echo "[Info] Installing native system packages via apt-get..."
    if command -v apt-get &> /dev/null; then
        sudo apt-get update
        sudo apt-get install -y \
            build-essential \
            cmake \
            ninja-build \
            pkg-config \
            libgrpc++-dev \
            libprotobuf-dev \
            protobuf-compiler \
            protobuf-compiler-grpc \
            libgtest-dev
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y \
            gcc-c++ \
            cmake \
            ninja-build \
            grpc-devel \
            grpc-plugins \
            protobuf-devel \
            gtest-devel
    else
        echo "[Error] Unsupported package manager for 'system' mode. Use 'conan' mode instead."
        exit 1
    fi

    echo "[Info] Configuring with system libraries..."
    cmake -S . -B build -G "Ninja" -DCMAKE_BUILD_TYPE=Release
    cmake --build build -j"$(nproc)"
else
    echo "[Info] Using Conan package manager (self-contained)..."
    if ! command -v conan &> /dev/null; then
        echo "[Info] Installing Conan via pip..."
        pip install --upgrade conan
    fi

    echo "[Info] Detecting host compiler profile..."
    conan profile detect --force

    echo "[Info] Installing C++ dependencies via Conan..."
    conan install . --output-folder=build -s build_type=Release --build=missing

    echo "[Info] Configuring project with CMake..."
    cmake -S . -B build -G "Ninja" \
        -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake \
        -DCMAKE_BUILD_TYPE=Release

    echo "[Info] Compiling libraries and executables..."
    cmake --build build -j"$(nproc)"
fi

echo "=========================================================="
echo "  Build successful! Running Automated Test Suite..."
echo "=========================================================="
./build/financial_pipeline_tests

echo "=========================================================="
echo "  Running Benchmark (1.0 GB)..."
echo "=========================================================="
./build/financial_pipeline_benchmark --gb 1.0

echo "=========================================================="
echo "  Linux setup and validation complete!"
echo "=========================================================="
