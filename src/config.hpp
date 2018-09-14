#ifndef CONFIG_HPP
#define CONFIG_HPP

#include "driver.hpp"

#include <cstdio>
#include <string>

struct Config {
  Config()
    : hosts("127.0.0.1")
    , type("select")
    , trusted_cert_file("trusted_cert.pem")
    , num_threads(1)
    , num_io_threads(1)
    , num_core_connections(1)
    , coalesce_delay(-1)
    , num_requests(10000000)
    , num_concurrent_requests(5000)
    , num_partition_keys(999)
    , data_size(1)
    , batch_size(1000)
    , protocol_version(0)
    , request_rate(1000)
    , log_level(CASS_LOG_ERROR)
    , sampling_rate(2000)
    , use_token_aware(true)
    , use_prepared(true)
    , use_ssl(false)
    , use_stdout(false) { }

  void from_cli(int argc, char** argv);
  void dump(FILE* file);
  std::string filename();

  std::string hosts;
  std::string type;
  std::string trusted_cert_file;
  std::string label;
  int num_threads;
  int num_io_threads;
  int num_core_connections;
  int coalesce_delay;
  int num_requests;
  int num_concurrent_requests;
  int num_partition_keys;
  int data_size;
  int batch_size;
  int protocol_version;
  int request_rate;
  CassLogLevel log_level;
  int sampling_rate;
  bool use_token_aware;
  bool use_prepared;
  bool use_ssl;
  bool use_stdout;
  std::string args_;
};

#endif // CONFIG_HPP
