#ifndef BENCHMARK_HPP
#define BENCHMARK_HPP

#include "barrier.hpp"
#include "config.hpp"
#include "driver.hpp"
#include "utils.hpp"

#include <uv.h>

#include <functional>
#include <string>
#include <vector>

class PerfTest {
public:
  PerfTest(CassSession* session,
           const std::string& query,
           const Config& config,
           Barrier& barrier)
    : session_(session)
    , query_(query)
    , data_(generate_data(config.data_size))
    , config_(config)
    , barrier_(barrier) { }

  virtual ~PerfTest();

  void setup();
  void run();
  void wait();

protected:
  virtual void on_run() = 0;
  virtual void on_setup() = 0;

private:
  static void on_thread(void* arg);

protected:
  CassSession* session() { return session_; }
  const std::string& query() { return query_; }
  const std::string& data() { return data_; }
  const CassPrepared* prepared() { return prepared_; }
  const Config& config() { return config_; }

  int num_requests();

protected:
  void notify_done() {
    barrier_.notify();
  }

  void wait_for_futures(const std::vector<CassFuture*>& futures,
                        std::function<void(const CassResult*)> verify_result);

private:
  CassSession* const session_;
  const std::string query_;
  const std::string data_;
  const CassPrepared* prepared_;
  const Config& config_;
  Barrier& barrier_;
  std::vector<uv_thread_t> threads_;
};

#endif // BENCHMARK_HPP
