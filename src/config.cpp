#include "config.hpp"

#include <algorithm>
#include <cstring>
#include <cstdio>
#include <cstdlib>

#define CHECK_ARG(name) do { \
if (i + 1 > argc) {\
  fprintf(stderr, #name " expects an argument\n"); \
  exit(-1); \
} \
} while (0)

void Config::from_cli(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    char* arg = argv[i];
    if (strcmp(arg, "--hosts") == 0) {
      CHECK_ARG("--hosts");
      hosts = argv[i + 1];

      i++;
    } else if (strcmp(arg, "--type") == 0) {
      CHECK_ARG("--type");
      type = argv[i + 1];
      std::transform(type.begin(), type.end(), type.begin(), ::tolower);
      i++;
    } else if (strcmp(arg, "--num-threads") == 0) {
      CHECK_ARG("--num-threads");
      num_threads = atoi(argv[i + 1]);
      if (num_threads <= 0) {
        fprintf(stderr, "--num-threads has the invalid value %d\n", num_threads);
        exit(-1);
      }
      i++;
    } else if (strcmp(arg, "--num-io-threads") == 0) {
      CHECK_ARG("--num-io-threads");
      num_io_threads = atoi(argv[i + 1]);
      if (num_io_threads <= 0) {
        fprintf(stderr, "--num-io-threads has the invalid value %d\n", num_io_threads);
        exit(-1);
      }
      i++;
    } else if (strcmp(arg, "--num-core-connections") == 0) {
      CHECK_ARG("--num-core-connections");
      num_core_connections = atoi(argv[i + 1]);
      if (num_core_connections <= 0) {
        fprintf(stderr, "--num-core-connections has the invalid value %d\n", num_core_connections);
        exit(-1);
      }
      i++;
    } else if (strcmp(arg, "--num-requests") == 0) {
      CHECK_ARG("--num-requests");
      num_requests = atoi(argv[i + 1]);
      if (num_requests <= 0) {
        fprintf(stderr, "--num-requests has the invalid value %d\n", num_requests);
        exit(-1);
      }
      i++;
    } else if (strcmp(arg, "--num-concurrent-requests") == 0) {
      CHECK_ARG("--num-concurrent-requests");
      num_concurrent_requests = atoi(argv[i + 1]);
      if (num_concurrent_requests <= 0) {
        fprintf(stderr, "--num-concurrent-requests has the invalid value %d\n", num_concurrent_requests);
        exit(-1);
      }
      i++;
    } else if (strcmp(arg, "--batch-size") == 0) {
      CHECK_ARG("--batch-size");
      batch_size = atoi(argv[i + 1]);
      if (batch_size <= 0) {
        fprintf(stderr, "--batch-size has the invalid value %d\n", batch_size);
        exit(-1);
      }
      i++;
    } else if (strcmp(arg, "--data-size") == 0) {
      CHECK_ARG("--data-size");
      data_size = atoi(argv[i + 1]);
      if (data_size <= 0) {
        fprintf(stderr, "--data-size has the invalid value %d\n", data_size);
        exit(-1);
      }
      i++;
    } else if (strcmp(arg, "--protocol-version") == 0) {
      CHECK_ARG("--protocol-version");
      protocol_version = atoi(argv[i + 1]);
      if (protocol_version <= 0) {
        fprintf(stderr, "--protocol-version has the invalid value %d\n", protocol_version);
        exit(-1);
      }
      i++;
    } else if (strcmp(arg, "--log-level") == 0) {
      CHECK_ARG("--log-level");
      int temp = atoi(argv[i + 1]);
      if (temp < CASS_LOG_DISABLED || temp > CASS_LOG_TRACE) {
        fprintf(stderr, "--log-level has the invalid value %d\n", temp);
        exit(-1);
      }
      log_level = static_cast<CassLogLevel>(temp);
      i++;
    } else if (strcmp(arg, "--sampling-rate") == 0) {
      CHECK_ARG("--sampling-rate");
      sampling_rate = atoi(argv[i + 1]);
      if (sampling_rate <= 0) {
        fprintf(stderr, "--sampling-rate has the invalid value %d\n", sampling_rate);
        exit(-1);
      }
      i++;
    } else if (strcmp(arg, "--use-token-aware") == 0) {
      CHECK_ARG("--use-token-aware");
      use_token_aware = atoi(argv[i + 1]);
      i++;
    } else if (strcmp(arg, "--use-prepared") == 0) {
      CHECK_ARG("--use-prepared");
      use_prepared = atoi(argv[i + 1]);
      i++;
    } else if (strcmp(arg, "--use-ssl") == 0) {
      CHECK_ARG("--use-ssl");
      use_ssl = atoi(argv[i + 1]);
      i++;
    } else if (strcmp(arg, "--trust-cert-file") == 0) {
      CHECK_ARG("--trust-cert-file");
      trusted_cert_file = argv[i + 1];
      i++;
    }
    else {
      fprintf(stderr, "%s is an invalid flags\n", arg);
      exit(-1);
    }
  }
}

void Config::dump() {
  fprintf(stderr,
          "Running driver version %d.%d.%d (%s) with hosts \"%s\"\n"
          "type: %s\n"
          "protocol version: %d (0 means use default)\n"
          "log_level: %s\n"
          "client threads: %d\n"
          "i/o threads: %d\n"
          "core connections: %d\n"
          "requests: %d\n"
          "concurrent requests: %d\n"
          "use_token_aware: %s\n"
          "use_prepared: %s\n"
          "use_ssl: %s\n"
          "data_size: %d\n"
          "batch size: %d\n"
          "sampling rate: %d\n\n" ,
        #ifdef DSE_VERSION_MAJOR
          DSE_VERSION_MAJOR, DSE_VERSION_MINOR, DSE_VERSION_PATCH,
          strlen(DSE_VERSION_SUFFIX) == 0 ? "<no suffix>" : DSE_VERSION_SUFFIX,
        #else
          CASS_VERSION_MAJOR, CASS_VERSION_MINOR, CASS_VERSION_PATCH,
          strlen(CASS_VERSION_SUFFIX) == 0 ? "<no suffix>" : CASS_VERSION_SUFFIX,
        #endif
          hosts.c_str(),
          type.c_str(),
          protocol_version,
          cass_log_level_string(log_level),
          num_threads,
          num_io_threads,
          num_core_connections,
          num_requests,
          num_concurrent_requests,
          use_token_aware ? "true" : "false",
          use_prepared ? "true" : "false",
          use_ssl ? "true" : "false",
          data_size,
          batch_size,
          sampling_rate);
}
