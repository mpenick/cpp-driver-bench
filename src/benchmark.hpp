#ifndef BENCHMARK_HPP
#define BENCHMARK_HPP

#include "barrier.hpp"
#include "config.hpp"
#include "driver.hpp"
#include "utils.hpp"

#include <uv.h>

#include <string>
#include <vector>

class Benchmark {
public:
  Benchmark(CassSession* session, const Config& config,
            const std::string& query, size_t parameter_count,
            bool is_threaded);

  virtual ~Benchmark();

  void setup();
  void run();
  bool poll(uint64_t timeout_ms);
  void join();

protected:
  virtual void on_setup() { } // Optional
  virtual void on_run() = 0;

private:
  static void on_thread(void* arg);

protected:
  CassSession* session() { return session_; }
  const std::string& query() { return query_; }
  size_t parameter_count() { return parameter_count_; }
  const std::string& data() { return data_; }
  const CassPrepared* prepared() { return prepared_; }
  const Config& config() { return config_; }

  int num_requests();

protected:
  void notify_done() {
    barrier_.notify();
  }

private:
  CassSession* const session_;
  const std::string query_;
  const size_t parameter_count_;
  const std::string data_;
  const CassPrepared* prepared_;
  const Config& config_;
  const bool is_threaded_;
  Barrier barrier_;
  std::vector<uv_thread_t> threads_;
};

#endif // BENCHMARK_HPP
