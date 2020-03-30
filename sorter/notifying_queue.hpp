#ifndef SORT__NOTIFYING_QUEUE_HPP_
#define SORT__NOTIFYING_QUEUE_HPP_

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <experimental/filesystem>
#include <fstream>

template <typename T>
T deserializeForStoringQueue(std::string&&);

template <typename T>
std::string serializeForStoringQueue(T&&);


namespace notifying_queue
{
//Theese containers are not exception safe, and it is enough for the task

template <typename T, bool file_storable = true>
class Queue
{
 public:
  template <bool f = file_storable, typename std::enable_if<f, int>::type = 0>
  Queue(const std::size_t capacity = 1024 * 1024) : capacity_{capacity} {}
  template <bool f = file_storable, typename std::enable_if<!f, int>::type = 0>
  Queue() {}

  Queue(const Queue&) = delete;
  Queue& operator=(const Queue&) = delete;

  bool waitAndPop(T& ret)
  {
    std::unique_lock lk{m_};
    cv_.wait(lk, [&size{size_}, &f{will_not_grow_}](){ return size || f.load();});

    if (size())
    {
      --size_;

      internalPop(ret);

      return true;
    }

    return false;
  }

  bool tryPop(T& ret)
  {
    std::lock_guard lk{m_};

    if (!size())
    {
      return false;
    }

    --size_;
    internalPop(ret);

    return true;
  }

  void push(T el)
  {
    if (will_not_grow_.load(std::memory_order_consume))
    {
      return;
    }

    std::lock_guard lk{m_};

    internalPush(std::move(el));
    ++size_;

    cv_.notify_one();
  }

  void finish() noexcept 
  {
    will_not_grow_.store(true);
    cv_.notify_all();
  }

 protected:
  std::size_t size() const noexcept
  {
    return size_;
  }

  //There is the contract that our container is not empty
  void internalPop(T& el)
  {
    el = std::move(queue_.front());
    queue_.pop();

    if constexpr (!file_storable)
    {
      return;
    }

    if (!queue_.empty())
    {
      return;
    }

    if (first_stored_file_num_ != -1)
    {
      const std::string file_name{file_names_prefix_ + std::to_string(first_stored_file_num_++)};
      std::ifstream file{file_name};
      for (int i = 0; i < capacity_; ++i)
      {
        std::string tmp;

	file >> tmp;

        queue_.push(deserializeForStoringQueue<T>(std::move(tmp)));
      }

      std::experimental::filesystem::remove(file_name);
      if (first_stored_file_num_ > last_stored_file_num_)
      {
        first_stored_file_num_ = last_stored_file_num_ = -1;
      }

      return;
    }


    for (; !queue_to_store_.empty(); queue_to_store_.pop())
    {
      queue_.push(std::move(queue_to_store_.front()));
    }
  }

  template <bool f = file_storable>
  typename std::enable_if<f, void>::type internalPush(T&& el)
  {
    if (!queue_to_store_.empty() || first_stored_file_num_ != -1)
    {
      if (queue_to_store_.size() < capacity_)
      {
        queue_to_store_.push(std::move(el));
	return;
      }

      if (first_stored_file_num_ == -1)
      {
        first_stored_file_num_ = 0;
      }

      std::ofstream store_file{file_names_prefix_ + std::to_string(++last_stored_file_num_)};

      for (; !queue_to_store_.empty(); queue_to_store_.pop())
      {
        store_file << serializeForStoringQueue(std::move(queue_to_store_.front()));
	store_file << '\n';
      }

      queue_to_store_.push(std::move(el));

      return;
    }

    if (queue_.size() < capacity_)
    {
      queue_.push(std::move(el));
      return;
    }

    queue_to_store_.push(std::move(el));
  }

  template <bool f = file_storable>
  typename std::enable_if<!f, void>::type internalPush(T&& el)
  {
    queue_.push(std::move(el));
  }

  inline static const std::string file_names_prefix_{"tmp_queue"};

  std::queue<T> queue_;

  std::queue<T> queue_to_store_;
  const std::size_t capacity_{};

  int first_stored_file_num_{-1};
  int last_stored_file_num_{-1};

  mutable std::mutex m_;
  std::condition_variable cv_;

  std::atomic_bool will_not_grow_{false};

  std::size_t size_{0};
};

template <typename T, bool file_storable = true>
class DoublePopQueue : public Queue<T, file_storable>
{
  using Base = Queue<T, file_storable>;
 public:
  using Base::waitAndPop;
  bool waitAndPop(T& first, T& second)
  {
    std::unique_lock lk{Base::m_};
    Base::cv_.wait(lk, [&size{Base::size_}, &f{Base::will_not_grow_}]() { return size > 1 || f.load(); });

    if (Base::will_not_grow_) {
      return false;
    }

    Base::size_ -= 2;

    Base::internalPop(first);
    Base::internalPop(second);

    return true;
  }

  void waitAndPopForce(T& ret)
  {
    std::unique_lock lk{Base::m_};
    Base::cv_.wait(lk, [&q{Base::queue_}](){ return !q.empty();});

    --Base::size_;
    Base::internalPop(ret);
  }

  void pushForce(T el)
  {
    std::lock_guard lk{Base::m_};

    ++Base::size_;

    Base::internalPush(std::move(el));

    if (Base::queue_.size() > 1)
    {
      Base::cv_.notify_one();
    }
  }
};
}

#endif //SORT__NOTIFYING_QUEUE_HPP_
