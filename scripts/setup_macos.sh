#!/usr/bin/env bash
set -euo pipefail

echo "=========================================================="
echo "  Setting up Financial Pipeline on macOS (Apple Silicon / Intel)"
echo "=========================================================="

BUILD_MODE="${1:-brew}" # Options: "brew" (recommended for macOS) or "conan"

if [[ "$BUILD_MODE" == "brew" ]]; then
    echo "[Info] Checking for Homebrew..."
    if ! command -v brew &> /dev/null; then
        echo "[Error] Homebrew not found. Please install Homebrew from https://brew.sh or use 'conan' mode."
        exit 1
    fi

    echo "[Info] Installing dependencies via Homebrew..."
    brew install cmake ninja grpc protobuf googletest pkg-config

    # Set Homebrew prefix for Apple Silicon (/opt/homebrew) or Intel (/usr/local)
    BREW_PREFIX="$(brew --prefix)"
    export CMAKE_PREFIX_PATH="${BREW_PREFIX}:${BREW_PREFIX}/opt/grpc:${BREW_PREFIX}/opt/protobuf:${BREW_PREFIX}/opt/googletest"

    echo "[Info] Configuring CMake with Homebrew packages..."
    cmake -S . -B build -G "Ninja" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_PREFIX_PATH="${CMAKE_PREFIX_PATH}"

    echo "[Info] Compiling libraries and executables..."
    cmake --build build -j"$(sysctl -n hw.ncpu)"
else
    echo "[Info] Using Conan package manager on macOS..."
    if ! command -v conan &> /dev/null; then
        echo "[Info] Installing Conan via pip..."
        pip install --upgrade conan
    fi

    echo "[Info] Detecting host profile..."
    conan profile detect --force

    echo "[Info] Installing dependencies via Conan..."
    conan install . --output-folder=build -s build_type=Release --build=missing

    echo "[Info] Configuring CMake with Conan Toolchain..."
    cmake -S . -B build -G "Ninja" \
        -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake \
        -DCMAKE_BUILD_TYPE=Release

    echo "[Info] Compiling libraries and executables..."
    cmake --build build -j"$(sysctl -n hw.ncpu)"
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
echo "  macOS setup and validation complete!"
echo "=========================================================="
