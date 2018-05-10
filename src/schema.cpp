#include "schema.hpp"

#include "utils.hpp"

void prime_select_query_data(CassSession* session, const std::string& data) {
  CassStatement* statement = cass_statement_new(PRIMING_INSERT_QUERY, 1);
  cass_statement_bind_string_n(statement, 0, data.c_str(), data.size());
  CassFuture* future = cass_session_execute(session, statement);
  cass_statement_free(statement);
  CassError rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
    cass_future_free(future);
    exit(-1);
  }
  cass_future_free(future);
}
