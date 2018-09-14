#include "config.hpp"

#include "utils.hpp"

#include "date.h"

#include <algorithm>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <sstream>

#define CHECK_ARG(name) do { \
if (i + 1 >= argc) { \
  fprintf(stderr, #name " expects an argument\n"); \
  exit(-1); \
} \
} while (0)

void Config::from_cli(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    if (i > 1) {
      args_.append(" ");
    }
    args_.append(argv[i]);
  }

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
    } else if (strcmp(arg, "--label") == 0) {
      CHECK_ARG("--label");
      label = argv[i + 1];
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
    } else if (strcmp(arg, "--coalesce-delay") == 0) {
      CHECK_ARG("--coalesce-delay");
      coalesce_delay = atoi(argv[i + 1]);
      if (coalesce_delay <= 0) {
        fprintf(stderr, "--coalesce-delay has the invalid value %d\n", coalesce_delay);
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
    } else if (strcmp(arg, "--num-partition-keys") == 0) {
      CHECK_ARG("--num-partition-keys");
      num_partition_keys = atoi(argv[i + 1]);
      if (num_partition_keys <= 0) {
        fprintf(stderr, "--num-partition-keys has the invalid value %d\n", num_partition_keys);
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
    } else if (strcmp(arg, "--request-rate") == 0) {
      CHECK_ARG("--request-rate");
      request_rate = atoi(argv[i + 1]);
      if (request_rate < 0) {
        fprintf(stderr, "--request-rate has the invalid value %d\n", request_rate);
        exit(-1);
      }
      i++;
    } else if (strcmp(arg, "--protocol-version") == 0) {
      CHECK_ARG("--protocol-version");
      protocol_version = atoi(argv[i + 1]);
      if (protocol_version < 0) {
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
      use_prepared = atoi(argv[i + 1]) != 0;
      i++;
    } else if (strcmp(arg, "--use-ssl") == 0) {
      CHECK_ARG("--use-ssl");
      use_ssl = atoi(argv[i + 1]) != 0;
      i++;
    } else if (strcmp(arg, "--use-stdout") == 0) {
      CHECK_ARG("--use-stdout");
      use_stdout = atoi(argv[i + 1]) != 0;
      i++;
    } else if (strcmp(arg, "--trust-cert-file") == 0) {
      CHECK_ARG("--trust-cert-file");
      trusted_cert_file = argv[i + 1] != 0;
      i++;
    }
    else {
      fprintf(stderr, "%s is an invalid flags\n", arg);
      exit(-1);
    }
  }
}

void Config::dump(FILE* file) {
  fprintf(file, "\ncli-arguments\n%s\n",
          args_.empty() ? "Using defaults" : args_.c_str());
  fprintf(file, "\ncli-full-arguments\n"
                "--hosts \"%s\" --type %s --label \"%s\" --protocol-version %d "
                "--num-threads %d --num-io-threads %d --num-core-connections %d --coalesce-delay %d --num-requests %d --num-concurrent-requests %d "
                "--num-partition-keys %d --data-size %d --request-rate %d --batch-size %d --log-level %d --sampling-rate %d "
                "--use-token-aware %d --use-prepared %d --use-ssl %d --use-stdout %d\n",
          hosts.c_str(), type.c_str(), label.c_str(), protocol_version,
          num_threads, num_io_threads, num_core_connections, coalesce_delay, num_requests, num_concurrent_requests,
          num_partition_keys, data_size, request_rate, batch_size, static_cast<int>(log_level), sampling_rate,
          use_token_aware, use_prepared, use_ssl, use_stdout);
}

std::string Config::filename() {
  std::stringstream s;
  std::string date(date::format("%Y%m%d_%H%M%S", std::chrono::system_clock::now()));
  s << type
    << "_" << "v" << driver_version()
    << "_" << num_threads << "threads"
    << "_" << num_io_threads << "io_threads"
    << "_" << num_core_connections << "core_connections";

  if (!label.empty()) {
    s << "_" << label;
  }

  s << "_" << date.substr(0, date.find_first_of('.'))
    << ".csv";
  return s.str();
}
