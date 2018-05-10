#ifndef CHUNKING_BENCHMARK_HPP
#define CHUNKING_BENCHMARK_HPP

#include "benchmark.hpp"

class ChunkingPerfTest : public PerfTest {
public:
  ChunkingPerfTest(CassSession* session, const std::string& query,
                   const Config& config, Barrier& status)
    : PerfTest(session, query, config, status) { }

protected:
  void run_requests(int num_paramters,
                    std::function<void(CassStatement*)> bind_params,
                    std::function<void(const CassResult*)> verify_result);

private:
  void run_chunk(int num_parameters,
                 int chunk_size,
                 std::vector<CassFuture*>& futures,
                 std::function<void(CassStatement*)> bind_params);
};

class SelectChunkingPerfTest : public ChunkingPerfTest {
public:
  SelectChunkingPerfTest(CassSession* session, const Config& config, Barrier& status);

  virtual void on_setup();
  virtual void on_run();
};

class InsertChunkingPerfTest : public ChunkingPerfTest {
public:
  InsertChunkingPerfTest(CassSession* session, const Config& config, Barrier& status);

  virtual void on_setup() { }

  virtual void on_run();
};

#endif // CHUNKING_BENCHMARK_HPP
