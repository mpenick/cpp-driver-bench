#ifndef CONFIG_HPP
#define CONFIG_HPP

#include "driver.hpp"

#include <string>

struct Config {
  Config()
    : hosts("127.0.0.1")
    , type("select")
    , trusted_cert_file("trusted_cert.pem")
    , num_threads(1)
    , num_io_threads(1)
    , num_core_connections(1)
    , num_requests(10000000)
    , num_concurrent_requests(10000)
    , data_size(1)
    , batch_size(1000)
    , protocol_version(0)
    , log_level(CASS_LOG_ERROR)
    , use_token_aware(true)
    , use_prepared(true)
    , use_ssl(false)
    , sampling_rate(2) { }

  void from_cli(int argc, char** argv);
  void dump();

  std::string hosts;
  std::string type;
  std::string trusted_cert_file;
  int num_threads;
  int num_io_threads;
  int num_core_connections;
  int num_requests;
  int num_concurrent_requests;
  int data_size;
  int batch_size;
  int protocol_version;
  CassLogLevel log_level;
  bool use_token_aware;
  bool use_prepared;
  bool use_ssl;
  int sampling_rate;
};

#endif // CONFIG_HPP
