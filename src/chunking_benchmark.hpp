#ifndef CHUNKING_BENCHMARK_HPP
#define CHUNKING_BENCHMARK_HPP

#include "benchmark.hpp"

class ChunkingBenchmark : public Benchmark {
public:
  ChunkingBenchmark(CassSession* session, const Config& config,
                    const std::string& query, size_t parameter_count);

  bool is_threaded() { return true; }

  virtual void on_run();

protected:
  virtual void bind_params(CassStatement* statement) = 0;
  virtual void verify_result(const CassResult* result) = 0;
};

class SelectChunkingBenchmark : public ChunkingBenchmark {
public:
  SelectChunkingBenchmark(CassSession* session, const Config& config);

  virtual void on_setup();

  virtual void bind_params(CassStatement* statement);
  virtual void verify_result(const CassResult* result);
};

class InsertChunkingBenchmark : public ChunkingBenchmark {
public:
  InsertChunkingBenchmark(CassSession* session, const Config& config);

  virtual void bind_params(CassStatement* statement);
  virtual void verify_result(const CassResult* result);
};

#endif // CHUNKING_BENCHMARK_HPP
