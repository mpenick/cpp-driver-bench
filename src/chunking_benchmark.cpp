#include "chunking_benchmark.hpp"

#include "schema.hpp"

ChunkingBenchmark::ChunkingBenchmark(CassSession* session, const Config& config,
                                     const std::string& query, size_t parameter_count)
  : Benchmark(session, config, query, parameter_count, true) { }

void ChunkingBenchmark::on_run() {
  std::vector<CassFuture*> futures;

  int request_count = num_requests();

  while(request_count > 0) {
    int chunk_size = std::min(request_count, config().num_concurrent_requests);

    futures.clear();
    futures.reserve(chunk_size);

    for (int i = 0; i < chunk_size; ++i) {
      CassStatement* statement;
      if (prepared() != NULL) {
        statement = cass_prepared_bind(prepared());
      } else {
        statement = cass_statement_new(query().c_str(), parameter_count());
      }
      cass_statement_set_is_idempotent(statement, cass_true);
      bind_params(statement);
      futures.push_back(cass_session_execute(session(), statement));
      cass_statement_free(statement);
    }


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

    request_count -= chunk_size;
  }

  notify_done();
}

SelectChunkingBenchmark::SelectChunkingBenchmark(CassSession* session, const Config& config)
  : ChunkingBenchmark(session, config, SELECT_QUERY, 0) { }

void SelectChunkingBenchmark::on_setup() {
  prime_select_query_data(session(), data());
}

void SelectChunkingBenchmark::bind_params(CassStatement* statement) {
  // No parameters
}

void SelectChunkingBenchmark::verify_result(const CassResult* result) {
  if (cass_result_column_count(result) != 2) {
    fprintf(stderr, "Result has invalid column count\n");
  }
}

InsertChunkingBenchmark::InsertChunkingBenchmark(CassSession* session, const Config& config)
  : ChunkingBenchmark(session, config, INSERT_QUERY, 2) { }

void InsertChunkingBenchmark::bind_params(CassStatement* statement) {
  CassUuid uuid;
  cass_uuid_gen_random(uuid_gen.get(), &uuid);
  cass_statement_bind_uuid(statement, 0, uuid);
  cass_statement_bind_string_n(statement, 1, data().c_str(), data().size());
}

void InsertChunkingBenchmark::verify_result(const CassResult* result) {
  // No result
}
