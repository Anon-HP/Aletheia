# ==============================================================================
# Aletheia — Root Makefile
# Multi-language build configuration: C++ static library + Go integration
# ==============================================================================

# Toolchain configuration (configurable via environment or command line)
CXX       ?= g++
# Tunable flags. `make CXXFLAGS="-g -O0"` (or an environment CXXFLAGS) replaces these.
CXXFLAGS  ?= -Wall -Wextra -O2
# Flags the code needs regardless of CXXFLAGS (C++17: inline variables, <optional>).
# -fPIC is kept as a precaution; whether CGO Pattern B requires it is not verified.
CXX_REQUIRED := -std=c++17 -fPIC
# Emit .d files so that editing a header rebuilds every object that includes it.
DEPFLAGS     := -MMD -MP
AR        ?= ar
# `:=` rather than `?=`: GNU Make predefines ARFLAGS (as -rv), so `?=` would
# never take effect. `make ARFLAGS=...` on the command line still overrides.
ARFLAGS   := rcs
GO        ?= go

# Directory layout
CPP_DIR       := cpp
CPP_INC_DIR   := $(CPP_DIR)/include
CPP_SRC_DIR   := $(CPP_DIR)/src
CPP_CAPI_DIR  := $(CPP_DIR)/capi
CPP_BUILD_DIR := $(CPP_DIR)/build
CPP_LIB_DIR   := $(CPP_DIR)/lib

# Target static library
CORE_LIB      := $(CPP_LIB_DIR)/libaletheia_core.a

# Include search paths
INCLUDES      := -I$(CPP_INC_DIR) -I$(CPP_CAPI_DIR)

# Source and object discovery
CPP_SRCS      := $(wildcard $(CPP_SRC_DIR)/*.cpp) $(wildcard $(CPP_CAPI_DIR)/*.cpp)
CPP_OBJS      := $(patsubst $(CPP_DIR)/%.cpp,$(CPP_BUILD_DIR)/%.o,$(CPP_SRCS))

# C++ tests: every cpp/tests/test_*.cpp becomes its own executable linked
# against the core library, so new test files need no Makefile changes.
CPP_TEST_DIR  := $(CPP_DIR)/tests
CPP_TEST_SRCS := $(wildcard $(CPP_TEST_DIR)/test_*.cpp)
CPP_TEST_BINS := $(patsubst $(CPP_TEST_DIR)/%.cpp,$(CPP_BUILD_DIR)/tests/%,$(CPP_TEST_SRCS))

# Phony targets
.PHONY: all cpp-lib test clean help

# Default target: build the C++ core static library for Go linking
all: cpp-lib

# Help target explaining available rules
help:
	@echo "Aletheia Build Targets:"
	@echo "  all       - Build the C++ core static library ($(CORE_LIB))"
	@echo "  cpp-lib   - Compile C++ sources and create static library"
	@echo "  test      - Build and run every C++ test in $(CPP_TEST_DIR)/test_*.cpp"
	@echo "  clean     - Remove build artifacts, object files, and libraries"
	@echo "  help      - Display this usage help"

# Rule to archive object files into the static library
cpp-lib: $(CORE_LIB)

$(CORE_LIB): $(CPP_OBJS)
	@mkdir -p $(CPP_LIB_DIR)
	$(AR) $(ARFLAGS) $@ $^

# Pattern rule for compiling C++ sources in cpp/src/
$(CPP_BUILD_DIR)/src/%.o: $(CPP_SRC_DIR)/%.cpp
	@mkdir -p $(CPP_BUILD_DIR)/src
	$(CXX) $(CXX_REQUIRED) $(DEPFLAGS) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Pattern rule for compiling C ABI wrappers in cpp/capi/
$(CPP_BUILD_DIR)/capi/%.o: $(CPP_CAPI_DIR)/%.cpp
	@mkdir -p $(CPP_BUILD_DIR)/capi
	$(CXX) $(CXX_REQUIRED) $(DEPFLAGS) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Build each C++ test executable and run them all; stops at the first failure.
test: $(CPP_TEST_BINS)
	@if [ -z "$(CPP_TEST_BINS)" ]; then echo "No C++ tests found in $(CPP_TEST_DIR)"; fi
	@set -e; for t in $(CPP_TEST_BINS); do echo "== $$t"; ./$$t; done

$(CPP_BUILD_DIR)/tests/%: $(CPP_TEST_DIR)/%.cpp $(CORE_LIB)
	@mkdir -p $(CPP_BUILD_DIR)/tests
	$(CXX) $(CXX_REQUIRED) $(DEPFLAGS) $(CXXFLAGS) $(INCLUDES) $< $(CORE_LIB) -o $@

# Header dependency files produced by DEPFLAGS (absent on a clean tree).
-include $(CPP_OBJS:.o=.d) $(CPP_TEST_BINS:=.d)

# Clean target to remove compiled objects and binaries
clean:
	rm -rf $(CPP_BUILD_DIR) $(CPP_LIB_DIR)
	rm -f aletheia-node aletheia-client
