#ifndef SCHEMA_HPP
#define SCHEMA_HPP

#include "driver.hpp"

#include <string>

#define DROP_SCHEMA \
  "DROP KEYSPACE perf"

#define KEYSPACE_SCHEMA \
  "CREATE KEYSPACE IF NOT EXISTS perf WITH " \
  "replication = { 'class': 'SimpleStrategy', 'replication_factor': '1'}"

#define TABLE_SCHEMA \
  "CREATE TABLE IF NOT EXISTS " \
  "perf.table1 (key uuid PRIMARY KEY, value varchar)"

#define SELECT_QUERY \
  "SELECT * FROM perf.table1 " \
  "WHERE key = a98d21b2-1900-11e4-b97b-e5e358e71e0d"

#define PRIMING_INSERT_QUERY \
  "INSERT INTO perf.table1 (key, value) " \
  "VALUES (a98d21b2-1900-11e4-b97b-e5e358e71e0d, ?)"

#define INSERT_QUERY \
  "INSERT INTO perf.table1 (key, value) VALUES (?, ?)"""

void prime_select_query_data(CassSession* session, const std::string& data);

#endif // SCHEMA_HPP
