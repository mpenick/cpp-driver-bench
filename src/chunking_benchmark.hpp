#ifndef CHUNKING_BENCHMARK_HPP
#define CHUNKING_BENCHMARK_HPP

#include "benchmark.hpp"
#include "utils.hpp"

#include <atomic>
#include <vector>

class ChunkingBenchmark : public Benchmark {
public:
  ChunkingBenchmark(CassSession* session, const Config& config,
                    const std::string& query, size_t parameter_count);

  bool is_threaded() { return true; }

  virtual void on_run();

protected:
  virtual void bind_params(CassStatement* statement) const = 0;
  virtual void verify_result(const CassResult* result) const = 0;
};

class SelectChunkingBenchmark : public ChunkingBenchmark {
public:
  SelectChunkingBenchmark(CassSession* session, const Config& config);

  virtual void on_setup();

  virtual void bind_params(CassStatement* statement) const;
  virtual void verify_result(const CassResult* result) const;

private:
  std::vector<Uuid> partition_keys_;
  mutable std::atomic<size_t> index_;
};

class InsertChunkingBenchmark : public ChunkingBenchmark {
public:
  InsertChunkingBenchmark(CassSession* session, const Config& config);

  virtual void bind_params(CassStatement* statement) const;
  virtual void verify_result(const CassResult* result) const;
};

#endif // CHUNKING_BENCHMARK_HPP
