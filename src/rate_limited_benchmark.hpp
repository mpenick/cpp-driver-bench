#ifndef RATE_LIMITED_BENCHMARK_HPP
#define RATE_LIMITED_BENCHMARK_HPP

#include "benchmark.hpp"

#include <atomic>

class RateLimitedBenchmark : public Benchmark {
public:
  RateLimitedBenchmark(CassSession* session, const Config& config,
                    const std::string& query, size_t parameter_count);
  ~RateLimitedBenchmark();

  bool is_threaded() { return false; }

  virtual void on_run();

protected:
  virtual void bind_params(CassStatement* statement) const = 0;
  virtual void verify_result(const CassResult* result) const = 0;

private:
  void run_query();

  static void on_result(CassFuture* future, void* data);
  void handle_result(CassFuture* future);

private:
  const int request_count_;
  const uint64_t wait_time_;
  uv_mutex_t mutex_;
  int count_;
  int outstanding_count_;
};

class SelectRateLimitedBenchmark : public RateLimitedBenchmark {
public:
  SelectRateLimitedBenchmark(CassSession* session, const Config& config);

  virtual void on_setup();

  virtual void bind_params(CassStatement* statement) const;
  virtual void verify_result(const CassResult* result) const;

private:
  std::vector<Uuid> partition_keys_;
  mutable std::atomic<size_t> index_;
};

class InsertRateLimitedBenchmark : public RateLimitedBenchmark {
public:
  InsertRateLimitedBenchmark(CassSession* session, const Config& config);

  virtual void bind_params(CassStatement* statement) const;
  virtual void verify_result(const CassResult* result) const;
};

#endif // RATE_LIMITED_BENCHMARK_HPP
