#include "benchmark.hpp"

void PerfTest::wait_for_futures(const std::vector<CassFuture*>& futures,
                                std::function<void (const CassResult*)> verify_result) {
  for (auto future : futures) {
    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
      print_error(future);
    } else {
      const CassResult* result = cass_future_get_result(future);
      verify_result(result);
      cass_result_free(result);
    }
    cass_future_free(future);
  }
}

PerfTest::~PerfTest() {
  if (prepared_) {
    cass_prepared_free(prepared_);
  }
}

void PerfTest::setup() {
  if (config_.use_prepared) {
    if (prepare_query(session_, query_.c_str(), &prepared_) != 0) {
      exit(-1);
    }
  }
  on_setup();
}

void PerfTest::run() {
  if (config_.num_threads > 0) {
    threads_.resize(config_.num_threads);
    for (auto& thread : threads_) {
      uv_thread_create(&thread, on_thread, this);
    }
  } else {
    on_run();
  }
}

void PerfTest::wait() {
  for (auto& thread : threads_) {
    uv_thread_join(&thread);
  }
}

void PerfTest::on_thread(void* arg) {
  PerfTest* test = static_cast<PerfTest*>(arg);
  test->on_run();
}

int PerfTest::num_requests() {
  if (!threads_.empty()) {
    return config_.num_requests / threads_.size();
  }
  return config_.num_requests;
}
