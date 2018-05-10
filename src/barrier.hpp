#ifndef BARRIER_HPP
#define BARRIER_HPP

#include <uv.h>

class Barrier {
public:
  Barrier(int initial_count)
    : count_(initial_count) {
    uv_mutex_init(&mutex_);
    uv_cond_init(&cond_);
  }

  ~Barrier() {
    uv_mutex_destroy(&mutex_);
    uv_cond_destroy(&cond_);
  }

  void notify() {
    uv_mutex_lock(&mutex_);
    count_--;
    uv_cond_signal(&cond_);
    uv_mutex_unlock(&mutex_);
  }

  bool wait(uint64_t timeout_ms) {
    int count = 0;
    uv_mutex_lock(&mutex_);
    if (count_ > 0) {
      uv_cond_timedwait(&cond_, &mutex_, timeout_ms * 1000 * 1000);
      count = count_;
    }
    uv_mutex_unlock(&mutex_);
    return count > 0;
  }

private:
  uv_mutex_t mutex_;
  uv_cond_t cond_;
  int count_;
};

#endif // BARRIER_HPP
