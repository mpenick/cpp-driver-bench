#include "utils.hpp"

#include <cstring>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>

static std::unique_ptr<CassUuidGen, decltype(&cass_uuid_gen_free)> uuid_gen(
    cass_uuid_gen_new(),
    cass_uuid_gen_free);

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

int load_trusted_cert(const char* file_or_directory, CassSsl* ssl) {
  CassError rc;
  char* cert;
  long cert_size;
  size_t bytes_read;

  struct stat file_stat;
  if (stat(file_or_directory, &file_stat) == 0) {
    if (file_stat.st_mode & S_IFDIR) {
      return load_trusted_cert_directory(file_or_directory, ssl);
    }
  } else {
    fprintf(stderr, "Error loading certificate file '%s' is not a valid file\n", file_or_directory);
    return 0;
  }

  FILE *in = fopen(file_or_directory, "rb");
  if (in == NULL) {
    fprintf(stderr, "Error loading certificate file '%s'\n", file_or_directory);
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

int load_trusted_cert_directory(const char* directory, CassSsl* ssl) {
  DIR* dir = opendir(directory);
  struct dirent* dir_entry;
  int trust_cert_loaded = 0;
  while ((dir_entry = readdir(dir)) != NULL) {
    if (strcmp(dir_entry->d_name, ".") != 0 && strcmp(dir_entry->d_name, "..") != 0) {
      char full_name[256];
      snprintf(full_name, 256, "%s/%s", directory, dir_entry->d_name);
      int rc = load_trusted_cert(full_name, ssl);
      if (rc != 1) {
        return rc;
      }
      trust_cert_loaded = 1;
    }
  }

  closedir(dir);
  return trust_cert_loaded;
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

void close_session(CassSession* session) {
  CassFuture* future = cass_session_close(session);
  cass_future_wait(future);
  cass_future_free(future);
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

std::string driver_version() {
  std::stringstream s;

#ifdef DSE_VERSION_MAJOR
  s <<  DSE_VERSION_MAJOR << "." << DSE_VERSION_MINOR << "." <<  DSE_VERSION_PATCH;
  if (strlen(DSE_VERSION_SUFFIX) != 0) {
    s << "-" << DSE_VERSION_SUFFIX;
  }
#else
  s <<  CASS_VERSION_MAJOR << "." << CASS_VERSION_MINOR << "." <<  CASS_VERSION_PATCH;
  if (strlen(CASS_VERSION_SUFFIX) != 0) {
    s << "-" << CASS_VERSION_SUFFIX;
  }
#endif
  return s.str();
}

static int query_num_nodes(CassSession* session) {
  int count = 0; // Return zero if an error occurs

  CassStatement* statement = cass_statement_new("SELECT * FROM system.peers", 0);
  CassFuture* future = cass_session_execute(session, statement);
  CassError rc = cass_future_error_code(future);
  if (rc == CASS_OK) {
    const CassResult* result = cass_future_get_result(future);
    count = cass_result_row_count(result) + 1; // Add the node we're querying
    cass_result_free(result);
  } else {
    print_error(future);
  }

  cass_statement_free(statement);
  cass_future_free(future);

  return count;
}

ServerInfo query_server_info(CassSession* session) {
  ServerInfo info;

  // Determine the server type (Apache Cassandra or DSE)
  CassStatement* statement = cass_statement_new("SELECT dse_version FROM system.local", 0);
  CassFuture* future = cass_session_execute(session, statement);
  CassError rc = cass_future_error_code(future);
  if (rc == CASS_OK) {
    info.type = "dse";
  } else {
    cass_statement_free(statement);
    cass_future_free(future);
    statement = cass_statement_new("SELECT release_version FROM system.local", 0);
    future = cass_session_execute(session, statement);
    rc = cass_future_error_code(future);
    if (rc == CASS_OK) {
      info.type = "cassandra";
    } else {
      print_error(future);
    }
  }

  // Get the version number of the server
  if (rc == CASS_OK) {
    const CassResult* result = cass_future_get_result(future);
    const CassRow* row = cass_result_first_row(result);
    if (row) {
      const char* version;
      size_t version_length;
      cass_value_get_string(cass_row_get_column(row, 0),
                            &version, &version_length);
      info.version = std::string(version, version_length);
    } else {
      print_error(future);
    }

    cass_result_free(result);
  }

  info.num_nodes = query_num_nodes(session);

  cass_statement_free(statement);
  cass_future_free(future);

  return info;
}

Uuid generate_random_uuid() {
  Uuid uuid;
  cass_uuid_gen_random(uuid_gen.get(), uuid);
  return uuid;
}

MachineInfo machine_info(sigar_t* sigar) {
  MachineInfo machine_info;
  sigar_cpu_info_list_t cpu_info_list;
  sigar_mem_t mem;

  if (SIGAR_OK != sigar_cpu_info_list_get(sigar, &cpu_info_list)) {
    fprintf(stderr, "Unable to get machine info (processor information)\n");
    machine_info.mhz = 0;
    machine_info.cores = 0;
  }
  machine_info.vendor = cpu_info_list.data[0].vendor;
  machine_info.model = cpu_info_list.data[0].model;
  machine_info.mhz = cpu_info_list.data[0].mhz;
  machine_info.cores = cpu_info_list.data[0].total_cores;
  if (SIGAR_OK != sigar_mem_get(sigar, &mem)) {
    fprintf(stderr, "Unable to get machine info (RAM information)\n");
    machine_info.ram_in_bytes = 0;
  }
  machine_info.ram_in_bytes = mem.total;

  return machine_info;
}

ProcessInfo process_info(sigar_t* sigar, sigar_pid_t pid) {
  ProcessInfo process_info;
  sigar_proc_cpu_t proc_cpu;
  sigar_proc_mem_t proc_mem;

  if (SIGAR_OK != sigar_proc_cpu_get(sigar, pid, &proc_cpu)) {
    fprintf(stderr, "Unable to get memory usage\n");
    process_info.cpu_percentage = 0;
  }
  process_info.cpu_percentage = proc_cpu.percent;

  if (SIGAR_OK != sigar_proc_mem_get(sigar, pid, &proc_mem)) {
    fprintf(stderr, "Unable to get memory usage\n");
    process_info.mem_in_bytes = 0;
  }
  process_info.mem_in_bytes = static_cast<uint64_t>(proc_mem.resident);

  return process_info;
}

double stolen_percentage(sigar_t* sigar, sigar_cpu_t* previous) {
  sigar_cpu_t cpu;
  sigar_cpu_perc_t perc;

  if (SIGAR_OK != sigar_cpu_get(sigar, &cpu)) {
    fprintf(stderr, "Unable to get CPU information\n");
    return 0.0;
  }
  if (SIGAR_OK != sigar_cpu_perc_calculate(previous, &cpu, &perc)) {
    fprintf(stderr, "Unable to calculate stolen CPU percentage\n");
  }

  *previous = cpu;
  return perc.stolen;
}
