#ifndef UTILS_HPP
#define UTILS_HPP

#include "driver.hpp"

#include <string>
#include <cstdio>
#include <memory>

#if CASS_VERSION_MAJOR >= 2
#define STRING_PARAM(s) s
#else
#define STRING_PARAM(s) cass_string_init(s)
#endif

struct ServerInfo {
  std::string type;
  std::string version;
  int num_nodes;
};

struct Uuid {
  CassUuid uuid;
  operator CassUuid*() { return &uuid; }
  operator CassUuid() const { return uuid; }
};

Uuid generate_random_uuid();

inline std::string generate_data(int size) {
  return std::string(size, 'a');
}

std::string driver_version();
ServerInfo query_server_info(CassSession*);

void print_error(CassFuture* future);

CassError prepare_query(CassSession* session, const char* query, const CassPrepared** prepared);

int load_trusted_cert_file(const char* file, CassSsl* ssl);

CassError connect_session(CassSession* session, const CassCluster* cluster);

void close_session(CassSession* session);

CassError execute_query(CassSession* session, const char* query);

#endif // UTILS_HPP
