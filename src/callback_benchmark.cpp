#include "callback_benchmark.hpp"

#include "schema.hpp"

CallbackBenchmark::CallbackBenchmark(CassSession* session, const Config& config,
                                     const std::string& query, size_t parameter_count)
  : Benchmark(session, config, query, parameter_count, false)
  , request_count_(num_requests())
  , count_(0)
  , outstanding_count_(0) { }


void CallbackBenchmark::on_run() {
  for (int i = 0; i < std::min(request_count_, config().num_concurrent_requests); ++i) {
    if (!run_query()) {
      break;
    }
  }
}

bool CallbackBenchmark::run_query() {
  if (count_++ > request_count_) {
    return false;
  }
  outstanding_count_++;

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

  return true;
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

  int count = --outstanding_count_;
  if (!run_query() && count == 0) {
    notify_done();
  }
}

SelectCallbackBenchmark::SelectCallbackBenchmark(CassSession* session, const Config& config)
  : CallbackBenchmark(session, config, SELECT_QUERY, 0) { }

void SelectCallbackBenchmark::on_setup() {
  prime_select_query_data(session(), data());
}

void SelectCallbackBenchmark::bind_params(CassStatement* statement) {
  // No parameters
}

void SelectCallbackBenchmark::verify_result(const CassResult* result) {
  if (cass_result_column_count(result) != 2) {
    fprintf(stderr, "Result has invalid column count\n");
  }
}

InsertCallbackBenchmark::InsertCallbackBenchmark(CassSession* session, const Config& config)
  : CallbackBenchmark(session, config, INSERT_QUERY, 2) { }

void InsertCallbackBenchmark::bind_params(CassStatement* statement) {
  CassUuid uuid;
  cass_uuid_gen_random(uuid_gen.get(), &uuid);
  cass_statement_bind_uuid(statement, 0, uuid);
  cass_statement_bind_string_n(statement, 1, data().c_str(), data().size());
}

void InsertCallbackBenchmark::verify_result(const CassResult* result) {
  // No result
}
