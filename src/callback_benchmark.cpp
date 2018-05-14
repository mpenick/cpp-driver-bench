#include "callback_benchmark.hpp"

#include "schema.hpp"

CallbackBenchmark::CallbackBenchmark(CassSession* session, const Config& config,
                                     const std::string& query, size_t parameter_count)
  : Benchmark(session, config, query, parameter_count, false)
  , request_count_(num_requests())
  , count_(0)
  , outstanding_count_(0) {
  uv_mutex_init(&mutex_);
}

CallbackBenchmark::~CallbackBenchmark() {
  uv_mutex_destroy(&mutex_);
}

void CallbackBenchmark::on_run() {
  for (int i = 0; i < std::min(request_count_, config().num_concurrent_requests); ++i) {
    uv_mutex_lock(&mutex_);
    if (count_++ < request_count_) {
      outstanding_count_++;
      uv_mutex_unlock(&mutex_);
      run_query();
    } else {
      uv_mutex_unlock(&mutex_);
      break;
    }
  }
}

void CallbackBenchmark::run_query() {
  CassFuture* future;
  CassStatement* statement;
  if (prepared()) {
    statement = cass_prepared_bind(prepared());
  } else {
    statement = cass_statement_new(query().c_str(), 0);
  }
  cass_statement_set_is_idempotent(statement, cass_true);
  bind_params(statement);
  future = cass_session_execute(session(), statement);
  cass_future_set_callback(future, on_result, this);
  cass_future_free(future);
  cass_statement_free(statement);
}

void CallbackBenchmark::on_result(CassFuture* future, void* data) {
  CallbackBenchmark* benchmark = static_cast<CallbackBenchmark*>(data);
  benchmark->handle_result(future);
}

void CallbackBenchmark::handle_result(CassFuture* future) {
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
  if (count_++ < request_count_) {
    outstanding_count_++;
    uv_mutex_unlock(&mutex_);
    run_query();
  } else {
    uv_mutex_unlock(&mutex_);
    if (is_done) {
      notify_done();
    }
  }
}

SelectCallbackBenchmark::SelectCallbackBenchmark(CassSession* session, const Config& config)
  : CallbackBenchmark(session, config, SELECT_QUERY, 1)
  , index_(0) { }

void SelectCallbackBenchmark::on_setup() {
  partition_keys_.reserve(config().num_partition_keys);
  for (int i = 0; i < config().num_partition_keys; ++i) {
    partition_keys_.push_back(prime_select_query_data(session(), data()));
  }
}

void SelectCallbackBenchmark::bind_params(CassStatement* statement) const {
  cass_statement_bind_uuid(statement, 0, partition_keys_[index_++ % partition_keys_.size()]);
}

void SelectCallbackBenchmark::verify_result(const CassResult* result) const {
  if (cass_result_column_count(result) != 2) {
    fprintf(stderr, "Result has invalid column count\n");
  }
}

InsertCallbackBenchmark::InsertCallbackBenchmark(CassSession* session, const Config& config)
  : CallbackBenchmark(session, config, INSERT_QUERY, 2) { }

void InsertCallbackBenchmark::bind_params(CassStatement* statement) const {
  cass_statement_bind_uuid(statement, 0, generate_random_uuid());
  cass_statement_bind_string_n(statement, 1, data().c_str(), data().size());
}

void InsertCallbackBenchmark::verify_result(const CassResult* result) const {
  // No result
}
