#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <list>
#include <fstream>
#include <mutex>
#include <thread>
#include <experimental/filesystem>
#include <stdexcept>

#include <notifying_queue.hpp>

template <typename T>
T deserializeForStoringQueue(std::string&&);

template <>
std::string deserializeForStoringQueue<std::string>(std::string&& str)
{
  return std::move(str);
}

//It will not be called in our code, but it is too long to write Queue specialisation for std::string
template <>
std::vector<std::string> deserializeForStoringQueue<std::vector<std::string>>(std::string&&)
{
  return {};
}

template <typename T>
std::string serializeForStoringQueue(T&&);

template<>
std::string serializeForStoringQueue<std::string>(std::string&& str)
{
  return std::move(str);
}

template<>
std::string serializeForStoringQueue<std::vector<std::string>>(std::vector<std::string>&& arg)
{
  return {};
}

//This is the contract
constexpr std::size_t max_string_size{1000};
//This is not size of raw available memory,
// but is estimated num of strings that we can contain
constexpr std::size_t max_strings_in_memory{120000};
constexpr std::size_t files_queue_capacity{1024 * 1024};

void waitForFreeMemory(std::condition_variable& cv, std::atomic_size_t& blocks_in_memory,
                       const std::size_t max_blocks_in_memory)
{
  std::mutex m;
  std::unique_lock lk{m};
  cv.wait(lk, [&blocks_in_memory, max_blocks_in_memory]() noexcept {
    return blocks_in_memory.fetch_add(0, std::memory_order_relaxed) < max_blocks_in_memory;
  });
}

void waitAndReserve(std::condition_variable& cv, std::atomic_size_t& blocks_in_memory,
                    const std::size_t max_blocks_in_memory,
                    std::vector<std::string>& portion, const std::size_t size)
{
  waitForFreeMemory(cv, blocks_in_memory, max_blocks_in_memory);
  blocks_in_memory.fetch_add(1, std::memory_order_relaxed);
  portion.reserve(size);
}

void finalize(std::ifstream& file, const std::string& str, std::ofstream& target)
{
  target << str << '\n';

  for (;;)
  {
    std::string tmp;

    file >> tmp;
    if (file.eof())
    {
      break;
    }
    target << tmp << '\n';
  }
}

void merge(std::ifstream& file1, std::ifstream& file2, std::ofstream& target)
{
  std::string str1, str2;
  str1.reserve(1000);
  str2.reserve(1000);
  
  file1 >> str1;
  file2 >> str2;
  for (;;)
  {
    if (str1 < str2)
    {
      target << str1 << '\n';
      file1 >> str1;
      if (file1.eof())
      {
        finalize(file2, str2, target);
	return;
      }
    }
    else
    {
      target << str2 << '\n';
      file2 >> str2;
      if (file2.eof())
      {
        finalize(file1, str1, target);
	return;
      }
    }
  }
}

template <bool is_main_thread = false>
void processReduce(notifying_queue::DoublePopQueue<std::string>& files_queue,
                   std::atomic_size_t& files_enumerator,
                   std::atomic_size_t& num_of_remaining_files,
                   const std::size_t thread_num)
{
  for (;;)
  {
    std::string filename1;
    std::string filename2;

    const std::size_t remaining_files = num_of_remaining_files.fetch_add(0, std::memory_order_relaxed);

    if (remaining_files == 1)
    {
      files_queue.finish();
    }

    if constexpr (!is_main_thread)
    {
      if (remaining_files / 2 - 1 < thread_num)
      {
        break;
      }
    }

    if constexpr (is_main_thread)
    {
      if (!files_queue.waitAndPop(filename1, filename2))
      {
        files_queue.waitAndPopForce(filename1);

        std::experimental::filesystem::rename(filename1, "result");
        return;
      }
    }
    else
    {
      if (!files_queue.waitAndPop(filename1, filename2))
      {
        return;
      }
    }

    num_of_remaining_files.fetch_sub(1, std::memory_order_relaxed);

    std::string new_file_name{"tmp" + std::to_string(files_enumerator.fetch_add(1, std::memory_order_relaxed))};
    {
      std::ofstream target_file{new_file_name};
      std::ifstream file1{filename1}, file2{filename2};
     
      merge(file1, file2, target_file);
    }
    files_queue.push(std::move(new_file_name));

    std::experimental::filesystem::remove(filename1);
    std::experimental::filesystem::remove(filename2);
  }
}

struct ThreadAction
{
  void operator()()
  {
    for (;;)
    {
      {
        std::vector<std::string> portion;
        if (!map_queue.waitAndPop(portion))
        {
          break;
        }

        const std::size_t file_index = files_enumerator.fetch_add(1, std::memory_order_relaxed);

        std::string filename{"tmp" + std::to_string(file_index)};

        {
          std::ofstream file{filename};

          std::sort(std::begin(portion), std::end(portion));

          for (const auto &str : portion)
          {
            file << str << '\n';
          }
        }
        blocks_in_memory.fetch_sub(1, std::memory_order_relaxed);

        file_names.push(std::move(filename));
      }
      may_continue_reading.notify_one();
    }

    processReduce(file_names, files_enumerator, num_of_remaining_files, order_num);
  }
  std::atomic_size_t& num_of_remaining_files;
  notifying_queue::Queue<std::vector<std::string>, false>& map_queue;
  std::condition_variable& may_continue_reading;
  std::atomic_size_t& blocks_in_memory;
  notifying_queue::DoublePopQueue<std::string>& file_names;
  std::atomic_size_t& files_enumerator;

  const std::size_t order_num;
};

int main(const int argc, const char* const argv[])
{
  if (argc < 2)
  {
    std::cerr << "File name is needed" << std::endl;
    return -1;
  }

  std::fstream file{argv[1]};
  if (file.fail())
  {
    std::cerr << "Fail to open file " << argv[0] << std::endl;
    return -2;
  }

  const std::size_t num_of_threads = []() noexcept -> std::size_t {
    const unsigned hw_c = std::thread::hardware_concurrency();
    if (hw_c)
    {
      return hw_c;
    }
    return 4;
  }();
  const std::size_t strings_by_thread{max_strings_in_memory / num_of_threads};
  const std::size_t num_of_threads_to_create{num_of_threads - 1};


  //I could use byte buffers instead of vectors of strings (to avoid allocations), it might increase performance
  //But I guess allocations is not bottle neck here because io operations probably take much more time
  //And coding with bytes buffers would take 2 hours more
  notifying_queue::Queue<std::vector<std::string>, false> raw_strings_queue;
  std::condition_variable may_continue;
  std::atomic_size_t blocks_in_memory{0};

  std::atomic_size_t num_of_remaining_files{0};
  std::atomic_size_t files_enumerator{0};

  notifying_queue::DoublePopQueue<std::string> file_names{files_queue_capacity};

  for (std::size_t i{}; i < num_of_threads_to_create; ++i)
  {
    ThreadAction action{num_of_remaining_files,
                        raw_strings_queue, may_continue, blocks_in_memory, file_names,
                        files_enumerator, i + 1};

    std::thread t{action};
    t.detach();
  }

  {
    std::vector<std::string> strings_portion;
    waitAndReserve(may_continue, blocks_in_memory, num_of_threads,
                strings_portion, strings_by_thread);

    int cnt{};

    for (;;)
    {
      std::string current;

      file >> current;

      if (file.eof())
      {
        if (!strings_portion.empty())
        {
          num_of_remaining_files.fetch_add(1, std::memory_order_relaxed);
          raw_strings_queue.push(std::move(strings_portion));
        }
        break;
      }
      ++cnt;

      strings_portion.push_back(std::move(current));
      if (strings_portion.size() == strings_by_thread)
      {
        num_of_remaining_files.fetch_add(1, std::memory_order_relaxed);
        raw_strings_queue.push(std::move(strings_portion));
        waitAndReserve(may_continue, blocks_in_memory, num_of_threads,
                       strings_portion, strings_by_thread);
      }
    }

    raw_strings_queue.finish();

    waitForFreeMemory(may_continue, blocks_in_memory, num_of_threads);
  }

  //I'm not sure that multithread reduce will be always faster, it depends on current system, disk, cpu, la, ...
  //It need ti be profiled
  processReduce<true>(file_names, files_enumerator, num_of_remaining_files, 0);

  return 0;
}
