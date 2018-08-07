#ifndef SCHEMA_HPP
#define SCHEMA_HPP

#include "driver.hpp"
#include "utils.hpp"

#include <string>

#define KEYSPACE_SCHEMA \
  "CREATE KEYSPACE IF NOT EXISTS perf WITH " \
  "replication = { 'class': 'SimpleStrategy', 'replication_factor': '3'}"

#define TABLE_SCHEMA \
  "CREATE TABLE IF NOT EXISTS " \
  "perf.table1 (key uuid PRIMARY KEY, value varchar)"

#define TRUNCATE_TABLE \
  "TRUNCATE perf.table1"

#define SELECT_QUERY \
  "SELECT * FROM perf.table1 WHERE key = ?"

#define PRIMING_INSERT_QUERY \
  "INSERT INTO perf.table1 (key, value) VALUES (?, ?)"

#define INSERT_QUERY \
  "INSERT INTO perf.table1 (key, value) VALUES (?, ?)"""

Uuid prime_select_query_data(CassSession* session, const std::string& data);

#endif // SCHEMA_HPP
