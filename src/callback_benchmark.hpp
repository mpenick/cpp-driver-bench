#ifndef CALLBACK_BENCHMARK_HPP
#define CALLBACK_BENCHMARK_HPP

#include "benchmark.hpp"

#include <atomic>

class CallbackPerfTest : public PerfTest {
public:
  CallbackPerfTest(CassSession* session, const Config& config, Barrier& status)
    : PerfTest(session, "", config, status)
    , request_count_(num_requests())
    , count_(0) { }

  virtual void on_run() {
  }

  bool is_done() {
    if (count_++ > request_count_) {
      return true;
    }
    return false;
  }

private:
  const int request_count_;
  std::atomic<int> count_;
};

#endif // CALLBACK_BENCHMARK_HPP
