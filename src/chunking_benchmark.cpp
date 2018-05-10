#include "chunking_benchmark.hpp"

#include "schema.hpp"

void ChunkingPerfTest::run_requests(int num_paramters,
                                    std::function<void (CassStatement*)> bind_params,
                                    std::function<void (const CassResult*)> verify_result) {
  std::vector<CassFuture*> futures;

  int request_count = num_requests();

  while(request_count > 0) {
    int chunk_size = std::min(request_count, config().num_concurrent_requests);

    run_chunk(num_paramters, chunk_size, futures, bind_params);
    wait_for_futures(futures, verify_result);

    request_count -= chunk_size;
  }

  notify_done();
}

void ChunkingPerfTest::run_chunk(int num_parameters,
                                 int chunk_size,
                                 std::vector<CassFuture*>& futures,
                                 std::function<void (CassStatement*)> bind_params) {
  futures.clear();
  futures.reserve(chunk_size);
  for (int i = 0; i < chunk_size; ++i) {
    CassStatement* statement;
    if (prepared() != NULL) {
      statement = cass_prepared_bind(prepared());
    } else {
      statement = cass_statement_new(query().c_str(), num_parameters);
    }
    cass_statement_set_is_idempotent(statement, cass_true);
    bind_params(statement);
    futures.push_back(cass_session_execute(session(), statement));
    cass_statement_free(statement);
  }
}

SelectChunkingPerfTest::SelectChunkingPerfTest(CassSession* session, const Config& config, Barrier& status)
  : ChunkingPerfTest(session, SELECT_QUERY, config, status) { }

void SelectChunkingPerfTest::on_setup() {
  CassStatement* statement = cass_statement_new(PRIMING_INSERT_QUERY, 1);
  cass_statement_bind_string_n(statement, 0, data().c_str(), data().size());
  CassFuture* future = cass_session_execute(session(), statement);
  cass_statement_free(statement);
  CassError rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
    cass_future_free(future);
    exit(-1);
  }
  cass_future_free(future);
}

void SelectChunkingPerfTest::on_run() {
  run_requests(0,
               [](CassStatement* statement) {
    // No parameters
  },
  [](const CassResult* result) {
    if (cass_result_column_count(result) != 2) {
      fprintf(stderr, "Invalid column count\n");
      exit(-1);
    }
  });
}

InsertChunkingPerfTest::InsertChunkingPerfTest(CassSession* session, const Config& config, Barrier& status)
  : ChunkingPerfTest(session, INSERT_QUERY, config, status) { }

void InsertChunkingPerfTest::on_run() {
  run_requests(0,
               [this](CassStatement* statement) {
    CassUuid uuid;
    cass_uuid_gen_random(uuid_gen.get(), &uuid);
    cass_statement_bind_uuid(statement, 0, uuid);
    cass_statement_bind_string_n(statement, 1, data().c_str(), data().size());
  },
  [](const CassResult* result) {
    // No result
  });
}
