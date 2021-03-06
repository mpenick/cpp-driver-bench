#include "utils.hpp"

#include <cstring>
#include <sstream>

static std::unique_ptr<CassUuidGen, decltype(&cass_uuid_gen_free)> uuid_gen(
    cass_uuid_gen_new(),
    cass_uuid_gen_free);

void print_error(CassFuture* future) {
#if CASS_VERSION_MAJOR >= 2
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
#else
  CassString message = cass_future_error_message(future);
  fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
#endif
}

CassError prepare_query(CassSession* session, const char* query, const CassPrepared** prepared) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;

  future = cass_session_prepare(session, STRING_PARAM(query));
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  else {
    *prepared = cass_future_get_prepared(future);
  }

  cass_future_free(future);

  return rc;
}

int load_trusted_cert_file(const char* file, CassSsl* ssl) {
  CassError rc;
  char* cert;
  long cert_size;
  size_t bytes_read;

  FILE *in = fopen(file, "rb");
  if (in == NULL) {
    fprintf(stderr, "Error loading certificate file '%s'\n", file);
    return 0;
  }

  fseek(in, 0, SEEK_END);
  cert_size = ftell(in);
  rewind(in);

  cert = (char*)malloc(cert_size);
  bytes_read = fread(cert, 1, cert_size, in);
  fclose(in);

  if (bytes_read == (size_t)cert_size) {
    rc = cass_ssl_add_trusted_cert_n(ssl, cert, cert_size);
    if (rc != CASS_OK) {
      fprintf(stderr, "Error loading SSL certificate: %s\n", cass_error_desc(rc));
      free(cert);
      return 0;
    }
  }

  free(cert);
  return 1;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

void close_session(CassSession* session) {
  CassFuture* future = cass_session_close(session);
  cass_future_wait(future);
  cass_future_free(future);
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(STRING_PARAM(query), 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

std::string driver_version() {
  std::stringstream s;

#ifdef DSE_VERSION_MAJOR
  s <<  DSE_VERSION_MAJOR << "." << DSE_VERSION_MINOR << "." <<  DSE_VERSION_PATCH;
  if (strlen(DSE_VERSION_SUFFIX) != 0) {
    s << "-" << DSE_VERSION_SUFFIX;
  }
#else
  s <<  CASS_VERSION_MAJOR << "." << CASS_VERSION_MINOR << "." <<  CASS_VERSION_PATCH;
  if (strlen(CASS_VERSION_SUFFIX) != 0) {
    s << "-" << CASS_VERSION_SUFFIX;
  }
#endif
  return s.str();
}

static int query_num_nodes(CassSession* session) {
  int count = 0; // Return zero if an error occurs

  CassStatement* statement = cass_statement_new("SELECT * FROM system.peers", 0);
  CassFuture* future = cass_session_execute(session, statement);
  CassError rc = cass_future_error_code(future);
  if (rc == CASS_OK) {
    const CassResult* result = cass_future_get_result(future);
    count = cass_result_row_count(result) + 1; // Add the node we're querying
    cass_result_free(result);
  } else {
    print_error(future);
  }

  cass_statement_free(statement);
  cass_future_free(future);

  return count;
}

ServerInfo query_server_info(CassSession* session) {
  ServerInfo info;

  // Determine the server type (Apache Cassandra or DSE)
  CassStatement* statement = cass_statement_new("SELECT dse_version FROM system.local", 0);
  CassFuture* future = cass_session_execute(session, statement);
  CassError rc = cass_future_error_code(future);
  if (rc == CASS_OK) {
    info.type = "dse";
  } else {
    cass_statement_free(statement);
    cass_future_free(future);
    statement = cass_statement_new("SELECT release_version FROM system.local", 0);
    future = cass_session_execute(session, statement);
    rc = cass_future_error_code(future);
    if (rc == CASS_OK) {
      info.type = "cassandra";
    } else {
      print_error(future);
    }
  }

  // Get the version number of the server
  if (rc == CASS_OK) {
    const CassResult* result = cass_future_get_result(future);
    const CassRow* row = cass_result_first_row(result);
    if (row) {
      const char* version;
      size_t version_length;
      cass_value_get_string(cass_row_get_column(row, 0),
                            &version, &version_length);
      info.version = std::string(version, version_length);
    } else {
      print_error(future);
    }

    cass_result_free(result);
  }

  info.num_nodes = query_num_nodes(session);

  cass_statement_free(statement);
  cass_future_free(future);

  return info;
}

Uuid generate_random_uuid() {
  Uuid uuid;
  cass_uuid_gen_random(uuid_gen.get(), uuid);
  return uuid;
}
