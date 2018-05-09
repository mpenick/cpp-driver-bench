#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <uv.h>

#include "date.h"
#include "dse.h"

/*
 * Use this example with caution. It's just used as a scratch example for debugging and
 * roughly analyzing performance.
 */

#define DROP_SCHEMA \
  "DROP KEYSPACE perf"

#define KEYSPACE_SCHEMA \
  "CREATE KEYSPACE IF NOT EXISTS perf WITH " \
  "replication = { 'class': 'SimpleStrategy', 'replication_factor': '1'}"

#define TABLE_SCHEMA \
  "CREATE TABLE IF NOT EXISTS " \
  "perf.table1 (key uuid PRIMARY KEY, value varchar)"

#if CASS_VERSION_MAJOR >= 2
#define STRING_PARAM(s) s
#else
#define STRING_PARAM(s) cass_string_init(s)
#endif

#define CHECK_ARG(name) do { \
if (i + 1 > argc) {\
  fprintf(stderr, #name " expects an argument\n"); \
  exit(-1); \
} \
} while (0)

std::unique_ptr<CassUuidGen, decltype(&cass_uuid_gen_free)> uuid_gen(
    cass_uuid_gen_new(),
    cass_uuid_gen_free);

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

  void dump() {
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

  void from_cli(int argc, char** argv) {
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

class Status {
public:
  Status(int initial_count)
    : count_(initial_count) {
    uv_mutex_init(&mutex_);
    uv_cond_init(&cond_);
  }

  ~Status() {
    uv_mutex_destroy(&mutex_);
    uv_cond_destroy(&cond_);
  }

  void notify() {
    uv_mutex_lock(&mutex_);
    count_--;
    uv_cond_signal(&cond_);
    uv_mutex_unlock(&mutex_);
  }

  int wait(uint64_t timeout_secs) {
    int count = 0;
    uv_mutex_lock(&mutex_);
    if (count_ > 0) {
      uv_cond_timedwait(&cond_, &mutex_, timeout_secs * 1000 * 1000 * 1000);
      count = count_;
    }
    uv_mutex_unlock(&mutex_);
    return count;
  }

private:
  uv_mutex_t mutex_;
  uv_cond_t cond_;
  int count_;
};

std::string generate_data(int size) {
  return std::string(size, 'a');
}

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

CassCluster* create_cluster(const Config& config) {
  CassCluster* cluster = cass_cluster_new();

  cass_cluster_set_contact_points(cluster, config.hosts.c_str());

#if 0
  CassRetryPolicy* retry_policy = cass_retry_policy_fallthrough_new();
  cass_cluster_set_retry_policy(cluster, retry_policy);
  cass_retry_policy_free(retry_policy);

  // This is unnecessary, the optimial protocol version is negotiated
  //cass_cluster_set_protocol_version(cluster, 2);

  cass_cluster_set_latency_aware_routing(cluster, cass_true);
#endif

  cass_cluster_set_connect_timeout(cluster, 10000);
  cass_cluster_set_reconnect_wait_time(cluster, 5000);
  cass_cluster_set_tcp_keepalive(cluster, cass_true, 15);

  if (config.use_ssl) {
    CassSsl* ssl = cass_ssl_new();
    cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_PEER_CERT);
    if (!load_trusted_cert_file(config.trusted_cert_file.c_str(), ssl)) {
      fprintf(stderr, "Failed to load certificate '%s' disabling peer verification\n",
              config.trusted_cert_file.c_str());
      cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_NONE);
    }
    cass_cluster_set_ssl(cluster, ssl);
    cass_ssl_free(ssl);
  }

  if (config.protocol_version > 0 && cass_cluster_set_protocol_version(cluster, config.protocol_version) != CASS_OK) {
    fprintf(stderr, "protocol version %d not supported using default\n", config.protocol_version);
  }

  cass_cluster_set_token_aware_routing(cluster, config.use_token_aware ? cass_true : cass_false);
  cass_cluster_set_num_threads_io(cluster, config.num_io_threads);
  cass_cluster_set_queue_size_io(cluster,
                                 config.num_concurrent_requests * std::max(config.num_threads, config.num_io_threads));

  cass_cluster_set_core_connections_per_host(cluster, config.num_core_connections);
  cass_cluster_set_max_connections_per_host(cluster, config.num_core_connections);

  cass_cluster_set_pending_requests_high_water_mark(cluster,
                                                    config.num_concurrent_requests * std::max(config.num_threads, config.num_io_threads));
  cass_cluster_set_write_bytes_high_water_mark(cluster, 16 * 1048576);

  return cluster;
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


class PerfTest {
public:
  PerfTest(CassSession* session,
           const std::string& query,
           const Config& config,
           Status& status)
    : session_(session)
    , query_(query)
    , data_(generate_data(config.data_size))
    , config_(config)
    , status_(status) { }

  virtual ~PerfTest() {
    if (prepared_) {
      cass_prepared_free(prepared_);
    }
  }

  void setup() {
    if (config_.use_prepared) {
      if (prepare_query(session_, query_.c_str(), &prepared_) != 0) {
        exit(-1);
      }
    }
    on_setup();
  }

  void run() {
    if (config_.num_threads > 0) {
      threads_.resize(config_.num_threads);
      for (auto& thread : threads_) {
        uv_thread_create(&thread, on_thread, this);
      }
    } else {
      on_run();
    }
  }

  void wait() {
    for (auto& thread : threads_) {
      uv_thread_join(&thread);
    }
  }

protected:
  virtual void on_run() = 0;
  virtual void on_setup() = 0;

private:
  static void on_thread(void* arg) {
    PerfTest* test = static_cast<PerfTest*>(arg);
    test->on_run();
  }

protected:
  CassSession* session() { return session_; }
  const std::string& query() { return query_; }
  const std::string& data() { return data_; }
  const CassPrepared* prepared() { return prepared_; }
  const Config& config() { return config_; }

  int num_requests() {
    if (!threads_.empty()) {
      return config_.num_requests / threads_.size();
    }
    return config_.num_requests;
  }

protected:
  void notify_done() {
    status_.notify();
  }

  void wait_for_futures(const std::vector<CassFuture*>& futures,
                        std::function<void(const CassResult*)> verify_result) {
    for (auto future : futures) {
      CassError rc = cass_future_error_code(future);
      if (rc != CASS_OK) {
        print_error(future);
      } else {
        const CassResult* result = cass_future_get_result(future);
        verify_result(result);
        cass_result_free(result);
      }
      cass_future_free(future);
    }
  }

private:
  CassSession* const session_;
  const std::string query_;
  const std::string data_;
  const CassPrepared* prepared_;
  const Config& config_;
  Status& status_;
  std::vector<uv_thread_t> threads_;
};

class ChunkingPerfTest : public PerfTest {
public:
  ChunkingPerfTest(CassSession* session, const std::string& query,
                   const Config& config, Status& status)
    : PerfTest(session, query, config, status) { }

protected:
  void run_requests(int num_paramters,
                    std::function<void(CassStatement*)> bind_params,
                    std::function<void(const CassResult*)> verify_result) {
    std::vector<CassFuture*> futures;

    int request_count = num_requests();

    while(request_count > 0) {
      int chunk_size = std::min(request_count, config().num_concurrent_requests);

      run_chunk(num_paramters, chunk_size, futures, bind_params);
      wait_for_futures(futures, verify_result);

      request_count -= chunk_size;
    }

    notify_done();
  }

private:
  void run_chunk(int num_parameters,
                 int chunk_size,
                 std::vector<CassFuture*>& futures,
                 std::function<void(CassStatement*)> bind_params) {
    futures.clear();
    futures.reserve(chunk_size);
    for (int i = 0; i < chunk_size; ++i) {
      CassStatement* statement;
      if (prepared() != NULL) {
        statement = cass_prepared_bind(prepared());
      } else {
        statement = cass_statement_new(query().c_str(), num_parameters);
      }
      cass_statement_set_is_idempotent(statement, cass_true);
      bind_params(statement);
      futures.push_back(cass_session_execute(session(), statement));
      cass_statement_free(statement);
    }
  }
};

#define SELECT_QUERY \
  "SELECT * FROM perf.table1 " \
  "WHERE key = a98d21b2-1900-11e4-b97b-e5e358e71e0d"

#define PRIMING_INSERT_QUERY \
  "INSERT INTO perf.table1 (key, value) " \
  "VALUES (a98d21b2-1900-11e4-b97b-e5e358e71e0d, ?)"

class SelectChunkingPerfTest : public ChunkingPerfTest {
public:
  SelectChunkingPerfTest(CassSession* session, const Config& config, Status& status)
    : ChunkingPerfTest(session, SELECT_QUERY, config, status) { }

  virtual void on_setup() {
    CassStatement* statement = cass_statement_new(PRIMING_INSERT_QUERY, 1);
    cass_statement_bind_string_n(statement, 0, data().c_str(), data().size());
    CassFuture* future = cass_session_execute(session(), statement);
    cass_statement_free(statement);
    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
      print_error(future);
      cass_future_free(future);
      exit(-1);
    }
    cass_future_free(future);
  }

  virtual void on_run() {
    run_requests(0,
    [](CassStatement* statement) {
      // No parameters
    },
    [](const CassResult* result) {
      if (cass_result_column_count(result) != 2) {
        fprintf(stderr, "Invalid column count\n");
        exit(-1);
      }
    });
  }
};

#define INSERT_QUERY \
  "INSERT INTO perf.table1 (key, value) VALUES (?, ?)"

class InsertChunkingPerfTest : public ChunkingPerfTest {
public:
  InsertChunkingPerfTest(CassSession* session, const Config& config, Status& status)
    : ChunkingPerfTest(session, INSERT_QUERY, config, status) { }

  virtual void on_setup() { }

  virtual void on_run() {
    run_requests(0,
    [this](CassStatement* statement) {
      CassUuid uuid;
      cass_uuid_gen_random(uuid_gen.get(), &uuid);
      cass_statement_bind_uuid(statement, 0, uuid);
      cass_statement_bind_string_n(statement, 1, data().c_str(), data().size());
    },
    [](const CassResult* result) {
      // No result
    });
  }
};

class CallbackPerfTest : public PerfTest {
public:
  CallbackPerfTest(CassSession* session, const Config& config, Status& status)
    : PerfTest(session, SELECT_QUERY, config, status)
    , request_count_(num_requests())
    , count_(0) { }

  virtual void on_run() {
  }

  bool is_done() {
    if (count_++ > request_count_) {
      return true;
    }
    return false;
  }

private:
  const int request_count_;
  std::atomic<int> count_;
};

int main(int argc, char** argv) {
  Config config;
  std::unique_ptr<PerfTest> test;

  config.from_cli(argc, argv);
  config.dump();

  cass_log_set_level(config.log_level);

  std::unique_ptr<CassCluster, decltype(&cass_cluster_free)> cluster(
        create_cluster(config), cass_cluster_free);
  std::unique_ptr<CassSession, decltype(&cass_session_free)> session(
        cass_session_new(), cass_session_free);

  if (connect_session(session.get(), cluster.get()) != CASS_OK) {
    return -1;
  }

  Status status(config.num_threads);

  if (config.type == "select") {
    test.reset(new SelectChunkingPerfTest(session.get(), config, status));
  } else if (config.type == "insert") {
    test.reset(new InsertChunkingPerfTest(session.get(), config, status));
  } else {
    fprintf(stderr, "Invalid test type: %s\n", config.type.c_str());
    return -1;
  }

  execute_query(session.get(), DROP_SCHEMA);
  execute_query(session.get(), KEYSPACE_SCHEMA);
  execute_query(session.get(), TABLE_SCHEMA);

  test->setup();

  uint64_t start = uv_hrtime();

  test->run();

  printf("%30s, "
         "%10s, %10s, %10s, %10s, "
         "%10s, %10s, %10s, %10s, "
         "%10s, %10s, %10s, %10s, "
         "%10s\n",
         "timestamp",
         "mean rate", "1m rate", "5m rate", "10m rate",
         "min", "mean", "median", "75th",
         "95th", "98th", "99th", "99.9th",
         "max");

  while (status.wait(config.sampling_rate)) {
#if CASS_VERSION_MAJOR >= 2
    CassMetrics metrics;
    cass_session_get_metrics(session.get(), &metrics);
    std::string date = date::format("%F %T", std::chrono::system_clock::now());
    printf("%30s, "
           "%10g, %10g, %10g, %10g, "
           "%10llu, %10llu, %10llu, %10llu, "
           "%10llu, %10llu, %10llu, %10llu, "
           "%10llu\n",
           date.c_str(),
           metrics.requests.mean_rate, metrics.requests.one_minute_rate,
           metrics.requests.five_minute_rate, metrics.requests.fifteen_minute_rate,
           (unsigned long long int)metrics.requests.min, (unsigned long long int)metrics.requests.median,
           (unsigned long long int)metrics.requests.mean, (unsigned long long int)metrics.requests.percentile_75th,
           (unsigned long long int)metrics.requests.percentile_95th, (unsigned long long int)metrics.requests.percentile_98th,
           (unsigned long long int)metrics.requests.percentile_99th, (unsigned long long int)metrics.requests.percentile_999th,
           (unsigned long long int)metrics.requests.max);
#endif
  }

  double elapsed_secs = (uv_hrtime() - start) / (1000.0 * 1000.0 * 1000.0);

  fprintf(stderr,
          "\nRan %d requests in %f seconds (%f requests/s)\n",
          config.num_requests, elapsed_secs, config.num_requests / elapsed_secs);

  test->wait();

  return 0;
}
