#include "rate_limited_benchmark.hpp"

#include "schema.hpp"
#include <unistd.h>
#include <sched.h>

static void escape(void* p) {
  asm volatile("" : : "g"(p) : "memory");
}

static uint64_t fast_clock() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000 * 1000 * 1000 + static_cast<uint64_t>(ts.tv_nsec);
}

RateLimitedBenchmark::RateLimitedBenchmark(CassSession* session, const Config& config,
                                           const std::string& query, size_t parameter_count)
  : Benchmark(session, config, query, parameter_count, true)
  , request_count_(config.num_requests)
  , wait_time_(config.request_rate > 0 ? (1000 * 1000 * 1000) / config.request_rate : 0)
  , count_(0)
  , outstanding_count_(0) {
  printf("####################################################### Wait nanos %lu\n", wait_time_);
  uv_mutex_init(&mutex_);
}

RateLimitedBenchmark::~RateLimitedBenchmark() {
  uv_mutex_destroy(&mutex_);
}

void RateLimitedBenchmark::on_run() {
  uint64_t expires = fast_clock() + wait_time_;
  while (true) {
    uv_mutex_lock(&mutex_);
    if (count_++ < request_count_) {
      outstanding_count_++;
      uv_mutex_unlock(&mutex_);
      run_query();
      /*
      uint64_t now = fast_clock();
      while (now < expires) {
        sched_yield();
        now = fast_clock();
      }
      expires = now + wait_time_;
      /*/
    } else {
      uv_mutex_unlock(&mutex_);
      break;
    }
  }
}

void RateLimitedBenchmark::run_query() {
  CassFuture* future;
  CassStatement* statement;
  if (prepared()) {
    statement = cass_prepared_bind(prepared());
  } else {
    statement = cass_statement_new(query().c_str(), parameter_count());
  }
  cass_statement_set_is_idempotent(statement, cass_true);
  bind_params(statement);
  future = cass_session_execute(session(), statement);
  cass_future_set_callback(future, on_result, this);
  cass_future_free(future);
  cass_statement_free(statement);
}

void RateLimitedBenchmark::on_result(CassFuture* future, void* data) {
  RateLimitedBenchmark* benchmark = static_cast<RateLimitedBenchmark*>(data);
  benchmark->handle_result(future);
}

void RateLimitedBenchmark::handle_result(CassFuture* future) {
  CassError rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    verify_result(result);
    cass_result_free(result);
  }

  uv_mutex_lock(&mutex_);
  bool is_done = --outstanding_count_ == 0;
  if (count_ >= request_count_) {
    uv_mutex_unlock(&mutex_);
    if (is_done) {
      for (int i = 0; i < config().num_threads; ++i) {
        notify_done();
      }
    }
  } else {
    uv_mutex_unlock(&mutex_);
  }
}

SelectRateLimitedBenchmark::SelectRateLimitedBenchmark(CassSession* session, const Config& config)
  : RateLimitedBenchmark(session, config, SELECT_QUERY, 1)
  , index_(0) { }

void SelectRateLimitedBenchmark::on_setup() {
  partition_keys_.reserve(config().num_partition_keys);
  for (int i = 0; i < config().num_partition_keys; ++i) {
    partition_keys_.push_back(prime_select_query_data(session(), data()));
  }
}

void SelectRateLimitedBenchmark::bind_params(CassStatement* statement) const {
  cass_statement_bind_uuid(statement, 0, partition_keys_[index_++ % partition_keys_.size()]);
}

void SelectRateLimitedBenchmark::verify_result(const CassResult* result) const {
  if (cass_result_column_count(result) != 2) {
    fprintf(stderr, "Result has invalid column count\n");
  }
}

InsertRateLimitedBenchmark::InsertRateLimitedBenchmark(CassSession* session, const Config& config)
  : RateLimitedBenchmark(session, config, INSERT_QUERY, 2) { }

void InsertRateLimitedBenchmark::bind_params(CassStatement* statement) const {
  cass_statement_bind_uuid(statement, 0, generate_random_uuid());
  cass_statement_bind_string_n(statement, 1, data().c_str(), data().size());
}

void InsertRateLimitedBenchmark::verify_result(const CassResult* result) const {
  // No result
}
