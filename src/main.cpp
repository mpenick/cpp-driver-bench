#include "barrier.hpp"
#include "config.hpp"
#include "callback_benchmark.hpp"
#include "chunking_benchmark.hpp"
#include "driver.hpp"
#include "schema.hpp"
#include "utils.hpp"

#include "date.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>

#include <uv.h>


CassCluster* create_cluster(const Config& config) {
  CassCluster* cluster = cass_cluster_new();

  cass_cluster_set_contact_points(cluster, config.hosts.c_str());

#if 0
  CassRetryPolicy* retry_policy = cass_retry_policy_fallthrough_new();
  cass_cluster_set_retry_policy(cluster, retry_policy);
  cass_retry_policy_free(retry_policy);

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
                                 2 * config.num_concurrent_requests * std::max(config.num_threads, config.num_io_threads));

  cass_cluster_set_core_connections_per_host(cluster, config.num_core_connections);
  cass_cluster_set_max_connections_per_host(cluster, config.num_core_connections);

  cass_cluster_set_pending_requests_high_water_mark(cluster,
                                                    2 * config.num_concurrent_requests * std::max(config.num_threads, config.num_io_threads));
  cass_cluster_set_write_bytes_high_water_mark(cluster, config.num_concurrent_requests * 16 * 1048576);

  return cluster;
}

int main(int argc, char** argv) {
  Config config;
  std::unique_ptr<Benchmark> benchmark;

  config.from_cli(argc, argv);

  cass_log_set_level(config.log_level);

  std::unique_ptr<CassCluster, decltype(&cass_cluster_free)> cluster(
        create_cluster(config), cass_cluster_free);
  std::unique_ptr<CassSession, decltype(&cass_session_free)> session(
        cass_session_new(), cass_session_free);

  if (config.type == "select") {
    benchmark.reset(new SelectChunkingBenchmark(session.get(), config));
  } else if (config.type == "insert") {
    benchmark.reset(new InsertChunkingBenchmark(session.get(), config));
  } else if (config.type == "selectcallback") {
    benchmark.reset(new SelectCallbackBenchmark(session.get(), config));
  } else if (config.type == "insertcallback") {
    benchmark.reset(new InsertCallbackBenchmark(session.get(), config));
  } else {
    fprintf(stderr, "Invalid test type: %s\n", config.type.c_str());
    return -1;
  }

  if (connect_session(session.get(), cluster.get()) != CASS_OK) {
    return -1;
  }

  execute_query(session.get(), DROP_SCHEMA);
  execute_query(session.get(), KEYSPACE_SCHEMA);
  execute_query(session.get(), TABLE_SCHEMA);

  benchmark->setup();

  uint64_t start = uv_hrtime();

  benchmark->run();

  std::string filename = config.filename();
  std::unique_ptr<FILE, decltype(&fclose)> file(
        config.use_stdout ? stdout : fopen(filename.c_str(), "w"),
        fclose);

  if (!file) {
    fprintf(stderr, "Unable to open output file: %s\n", filename.c_str());
    return -1;
  }

  config.dump(file.get());


  bool first = true;
  while (benchmark->poll(config.sampling_rate)) {
#if CASS_VERSION_MAJOR >= 2
    if (first) {
      fprintf(file.get(),
              "\n%-30s,"
              "%-10s ,%-10s ,%-10s ,%-10s ,"
              "%-10s ,%-10s ,%-10s ,%-10s ,"
              "%-10s ,%-10s ,%-10s ,%-10s ,"
              "%-10s\n",
              "timestamp",
              "mean rate", "1m rate", "5m rate", "10m rate",
              "min", "mean", "median", "75th",
              "95th", "98th", "99th", "99.9th",
              "max");
      first = false;
    }
    CassMetrics metrics;
    cass_session_get_metrics(session.get(), &metrics);
    std::string date(date::format("%F %T", std::chrono::system_clock::now()));
    fprintf(file.get(),
            "%30s, "
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

  fprintf(file.get(),
          "\n%-12s,%-10s ,%-10s\n"
          "%12d, %10g, %10g\n",
          "num_requests", "duration", "final rate",
          config.num_requests, elapsed_secs, config.num_requests / elapsed_secs);

  benchmark->join();

  return 0;
}
