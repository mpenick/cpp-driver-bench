#include "benchmark.hpp"

Benchmark::Benchmark(CassSession* session, const Config& config,
                     const std::string& query, size_t parameter_count,
                     bool is_threaded)
  : session_(session)
  , query_(query)
  , parameter_count_(parameter_count)
  , data_(generate_data(config.data_size))
  , config_(config)
  , is_threaded_(is_threaded)
  , barrier_(is_threaded_ ? config.num_threads : 1) { }

Benchmark::~Benchmark() {
  if (prepared_) {
    cass_prepared_free(prepared_);
  }
}

void Benchmark::setup() {
  if (config_.use_prepared) {
    if (prepare_query(session_, query_.c_str(), &prepared_) != 0) {
      exit(-1);
    }
  }
  on_setup();
}

void Benchmark::run() {
  if (is_threaded_) {
    threads_.resize(config_.num_threads);
    for (auto& thread : threads_) {
      uv_thread_create(&thread, on_thread, this);
    }
  } else {
    on_run();
  }
}

bool Benchmark::poll(uint64_t timeout_ms) {
  return barrier_.wait(timeout_ms);
}

void Benchmark::join() {
  for (auto& thread : threads_) {
    uv_thread_join(&thread);
  }
}

void Benchmark::on_thread(void* arg) {
  Benchmark* test = static_cast<Benchmark*>(arg);
  test->on_run();
}

int Benchmark::num_requests() {
  if (is_threaded_) {
    return config_.num_requests / threads_.size();
  }
  return config_.num_requests;
}
