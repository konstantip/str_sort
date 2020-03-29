#ifndef SORT__NOTIFYING_QUEUE_HPP_
#define SORT__NOTIFYING_QUEUE_HPP_

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <queue>

namespace notifying_queue
{
template <typename T>
class Queue
{
 public:
  Queue() = default;
  Queue(const Queue&) = delete;
  Queue& operator=(const Queue&) = delete;

  bool waitAndPop(T& ret)
  {
    std::unique_lock lk{m_};
    cv_.wait(lk, [&q{queue_}, &f{will_not_grow_}](){ return !q.empty() || f.load();});

    if (!queue_.empty())
    {
      ret = std::move(queue_.front());
      queue_.pop();

      return true;
    }

    return false;
  }

  bool tryPop(T& ret)
  {
    std::lock_guard lk{m_};

    if (queue_.empty())
    {
      return false;
    }

    ret = std::move(queue_.front());
    return true;
  }

  void push(T el)
  {
    std::lock_guard lk{m_};
    queue_.push(std::move(el));

    cv_.notify_one();
  }

  void finish() noexcept 
  {
    will_not_grow_.store(true);
    cv_.notify_all();
  }

 protected:
  std::queue<T> queue_;

  mutable std::mutex m_;
  std::condition_variable cv_;

  std::atomic_bool will_not_grow_{false};
};

template <typename T>
class DoublePopQueue : public Queue<T>
{
  using Base = Queue<T>;
 public:
  using Base::waitAndPop;
  bool waitAndPop(T& first, T& second)
  {
    std::unique_lock lk{Base::m_};
    Base::cv_.wait(lk, [&q{Base::queue_}, &f{Base::will_not_grow_}]() { return q.size() > 1 || f.load(); });

    if (Base::will_not_grow_) {
      return false;
    }

    //This is not exception safe, but ok for us, cause we want to crash that way
    first = std::move(Base::queue_.front());
    Base::queue_.pop();
    second = std::move(Base::queue_.front());
    Base::queue_.pop();
    return true;
  }

  void waitAndPopForce(T& ret)
  {
    std::unique_lock lk{Base::m_};
    Base::cv_.wait(lk, [&q{Base::queue_}](){ return !q.empty();});

    ret = std::move(Base::queue_.front());
    Base::queue_.pop();
  }

  void push(T el)
  {
    std::lock_guard lk{Base::m_};
    Base::queue_.push(std::move(el));

    if (Base::queue_.size() > 1)
    {
      Base::cv_.notify_one();
    }
  }
};
}

#endif //SORT__NOTIFYING_QUEUE_HPP_
