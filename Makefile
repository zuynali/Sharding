# =============================================================================
# shardkv — Sharded Key-Value Store with Two-Phase Commit
# =============================================================================
# Targets:
#   make           build all binaries
#   make test      build and run all unit tests
#   make clean     remove all build artifacts
#   make dirs      create build directories (called automatically)
#
# Binaries produced in bin/:
#   bin/shardkv        shard server
#   bin/shardkv-coord  coordinator
#   bin/shardkv-cli    client REPL
#   bin/ring_test      ring unit tests
# =============================================================================

CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -I src
LDFLAGS  :=

SRC := src
TST := tests
BIN := bin

# --------------------------------------------------------------------------
# Sources for each binary
# --------------------------------------------------------------------------

SHARD_SRC := $(SRC)/shard.cpp
COORD_SRC := $(SRC)/coord.cpp
CLI_SRC   := $(SRC)/cli.cpp
TEST_SRC  := $(TST)/ring_test.cpp

# Header-only files (not compiled directly, but tracked as dependencies)
HEADERS := $(SRC)/ring.hpp \
           $(SRC)/proto.hpp \
           $(SRC)/wal.hpp

# --------------------------------------------------------------------------
# Output binaries
# --------------------------------------------------------------------------

SHARD_BIN := $(BIN)/shardkv
COORD_BIN := $(BIN)/shardkv-coord
CLI_BIN   := $(BIN)/shardkv-cli
TEST_BIN  := $(BIN)/ring_test

ALL_BINS  := $(SHARD_BIN) $(COORD_BIN) $(CLI_BIN) $(TEST_BIN)

# --------------------------------------------------------------------------
# Phony targets
# --------------------------------------------------------------------------

.PHONY: all test clean dirs

# --------------------------------------------------------------------------
# Default target — build everything
# --------------------------------------------------------------------------

all: dirs $(ALL_BINS)

# --------------------------------------------------------------------------
# Create output directory
# --------------------------------------------------------------------------

dirs:
	@mkdir -p $(BIN)

# --------------------------------------------------------------------------
# Binary rules
# --------------------------------------------------------------------------

$(SHARD_BIN): $(SHARD_SRC) $(HEADERS) | dirs
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

$(COORD_BIN): $(COORD_SRC) $(HEADERS) | dirs
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

$(CLI_BIN): $(CLI_SRC) $(HEADERS) | dirs
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

$(TEST_BIN): $(TEST_SRC) $(SRC)/ring.hpp | dirs
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

# --------------------------------------------------------------------------
# Test target — build test binary and run it
# --------------------------------------------------------------------------

test: $(TEST_BIN)
	@echo ""
	@echo "Running ring unit tests..."
	@echo ""
	@./$(TEST_BIN); \
	EXIT=$$?; \
	echo ""; \
	exit $$EXIT

# --------------------------------------------------------------------------
# Clean
# --------------------------------------------------------------------------

clean:
	rm -rf $(BIN)
	@echo "Cleaned build artifacts."