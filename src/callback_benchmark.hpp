#ifndef CALLBACK_BENCHMARK_HPP
#define CALLBACK_BENCHMARK_HPP

#include "benchmark.hpp"

#include <atomic>

class CallbackBenchmark : public Benchmark {
public:
  CallbackBenchmark(CassSession* session, const Config& config,
                    const std::string& query, size_t parameter_count);

  bool is_threaded() { return false; }

  virtual void on_run();

protected:
  virtual void bind_params(CassStatement* statement) const = 0;
  virtual void verify_result(const CassResult* result) const = 0;

private:
  bool run_query();

  static void on_result(CassFuture* future, void* data);
  void handle_result(CassFuture* future);

private:
  const int request_count_;
  std::atomic<int> count_;
  std::atomic<int> outstanding_count_;
};

class SelectCallbackBenchmark : public CallbackBenchmark {
public:
  SelectCallbackBenchmark(CassSession* session, const Config& config);

  virtual void on_setup();

  virtual void bind_params(CassStatement* statement) const;
  virtual void verify_result(const CassResult* result) const;

private:
  std::vector<Uuid> partition_keys_;
  mutable std::atomic<size_t> index_;
};

class InsertCallbackBenchmark : public CallbackBenchmark {
public:
  InsertCallbackBenchmark(CassSession* session, const Config& config);

  virtual void bind_params(CassStatement* statement) const;
  virtual void verify_result(const CassResult* result) const;
};

#endif // CALLBACK_BENCHMARK_HPP
