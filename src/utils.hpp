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

extern std::unique_ptr<CassUuidGen, decltype(&cass_uuid_gen_free)> uuid_gen;

inline std::string generate_data(int size) {
  return std::string(size, 'a');
}

void print_error(CassFuture* future);

CassError prepare_query(CassSession* session, const char* query, const CassPrepared** prepared);

int load_trusted_cert_file(const char* file, CassSsl* ssl);

CassError connect_session(CassSession* session, const CassCluster* cluster);

CassError execute_query(CassSession* session, const char* query);

#endif // UTILS_HPP
