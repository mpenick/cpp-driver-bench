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

#include <unistd.h>
#include <inttypes.h>

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
    if (!load_trusted_cert(config.trusted_cert.c_str(), ssl)) {
      fprintf(stderr, "Failed to load certificate '%s' disabling peer verification\n",
              config.trusted_cert.c_str());
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
  sigar_t *sigar = NULL;
  sigar_cpu_t previous_cpu;
  sigar_pid_t pid;
  MachineInfo machine;

  sigar_open(&sigar);
  if (SIGAR_OK != sigar_cpu_get(sigar, &previous_cpu)) {
    fprintf(stderr, "Unable to get CPU information\n");
  }
  pid = sigar_pid_get(sigar);
  machine = machine_info(sigar);

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

  execute_query(session.get(), KEYSPACE_SCHEMA);
  execute_query(session.get(), TABLE_SCHEMA);
  execute_query(session.get(), TRUNCATE_TABLE);

  benchmark->setup();

  close_session(session.get());

  if (connect_session(session.get(), cluster.get()) != CASS_OK) {
    return -1;
  }

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

  std::string client_version = driver_version();
  ServerInfo server_info = query_server_info(session.get());
  std::string server_version = server_info.type + "-" + server_info.version;

  fprintf(file.get(),
          "\n%40s, %20s, %10s\n"
          "%40s, %20s, %10d\n",
          "driver version", "server version", "num nodes",
          client_version.c_str(), server_version.c_str(), server_info.num_nodes);

  fprintf(file.get(),
          "\n%40s, %40s, %10s, %10s, %20s\n"
          "%40s, %40s, %10d, %10d, %20" PRIu64 "\n",
          "cpu vendor", "cpu model", "mhz", "num cores", "ram",
          machine.vendor.c_str(), machine.model.c_str(), machine.mhz, machine.cores, machine.ram_in_bytes);

  bool first = true;
  while (benchmark->poll(config.sampling_rate)) {
    if (first) {
      fprintf(file.get(), "\n");
#if CASS_VERSION_MAJOR >= 2
      fprintf(file.get(),
              "%30s, "
              "%10s, %10s, %10s, %10s, "
              "%10s, %10s, %10s, %10s, "
              "%10s, %10s, %10s, %10s, "
              "%10s,",
              "timestamp",
              "mean rate", "1m rate", "5m rate", "10m rate",
              "min", "mean", "median", "75th",
              "95th", "98th", "99th", "99.9th",
              "max");
#endif
      fprintf(file.get(),
              "%15s, %20s, %15s\n", "cpu percentage", "stolen percentage", "memory usage");
      first = false;
    }
#if CASS_VERSION_MAJOR >= 2
    CassMetrics metrics;
    cass_session_get_metrics(session.get(), &metrics);
    std::string date(date::format("%F %T", std::chrono::system_clock::now()));
    fprintf(file.get(),
            "%30s, "
            "%10g, %10g, %10g, %10g, "
            "%10llu, %10llu, %10llu, %10llu, "
            "%10llu, %10llu, %10llu, %10llu, "
            "%10llu,",
            date.c_str(),
            metrics.requests.mean_rate, metrics.requests.one_minute_rate,
            metrics.requests.five_minute_rate, metrics.requests.fifteen_minute_rate,
            (unsigned long long int)metrics.requests.min, (unsigned long long int)metrics.requests.mean,
            (unsigned long long int)metrics.requests.median, (unsigned long long int)metrics.requests.percentile_75th,
            (unsigned long long int)metrics.requests.percentile_95th, (unsigned long long int)metrics.requests.percentile_98th,
            (unsigned long long int)metrics.requests.percentile_99th, (unsigned long long int)metrics.requests.percentile_999th,
            (unsigned long long int)metrics.requests.max);
#endif
    ProcessInfo process = process_info(sigar, pid);
    double stolen = stolen_percentage(sigar, &previous_cpu);
    fprintf(file.get(),
            "%15f, %20f, %15" PRIu64 "\n", process.cpu_percentage, stolen, process.mem_in_bytes);
  }

  double elapsed_secs = (uv_hrtime() - start) / (1000.0 * 1000.0 * 1000.0);

fprintf(file.get(),
        "\n%12s, %10s, %10s",
        "num requests", "duration", "final rate");
#if CASS_VERSION_MAJOR >= 2
  CassMetrics metrics;
  cass_session_get_metrics(session.get(), &metrics);

  fprintf(file.get(),
          ",%10s, %10s, %10s, %10s, "
          "%10s, %10s, %10s, %10s, "
          "%10s",
          "min", "mean", "median", "75th",
          "95th", "98th", "99th", "99.9th",
          "max");
#endif
  fprintf(file.get(), "\n");

  fprintf(file.get(),
          "%12d, %10g, %10g",
          config.num_requests, elapsed_secs, config.num_requests / elapsed_secs);
#if CASS_VERSION_MAJOR >= 2
  fprintf(file.get(),
          ",%10llu, %10llu, %10llu, %10llu, "
          "%10llu, %10llu, %10llu, %10llu, "
          "%10llu",
          (unsigned long long int)metrics.requests.min, (unsigned long long int)metrics.requests.mean,
          (unsigned long long int)metrics.requests.median, (unsigned long long int)metrics.requests.percentile_75th,
          (unsigned long long int)metrics.requests.percentile_95th, (unsigned long long int)metrics.requests.percentile_98th,
          (unsigned long long int)metrics.requests.percentile_99th, (unsigned long long int)metrics.requests.percentile_999th,
          (unsigned long long int)metrics.requests.max);
#endif
  fprintf(file.get(), "\n");

  benchmark->join();
  sigar_close(sigar);

  return 0;
}
