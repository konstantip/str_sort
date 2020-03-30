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

#include <cstring>

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

struct StringSet
{
  StringSet() = default;

  StringSet(const std::size_t size) : arr{new char[str_buffer_size * size]}, size{size} {}

  void clear() noexcept
  {
    for (std::size_t i = 0; i < size; ++i)
    {
      arr[i * str_buffer_size] = '\0';
    }
  }

  void allocate(const std::size_t new_size)
  {
    delete[] arr;
    arr = nullptr;

    size = new_size;
    arr = new char[str_buffer_size * size];
  }

  void swap(StringSet& other) noexcept
  {
    std::swap(arr, other.arr);
    std::swap(size, other.size);
  }

  StringSet(StringSet&& other) noexcept : StringSet()
  {
    swap(other);
  }

  StringSet& operator=(StringSet&& other) noexcept
  {
    if (&other == this)
    {
      return *this;
    }

    swap(other);

    return *this;
  }

  char* operator[](std::size_t n) noexcept
  {
    return arr + n * str_buffer_size;
  }

  ~StringSet()
  {
    delete[] arr;
  }

  char* arr{};
  std::size_t size{};

  static constexpr std::size_t str_buffer_size{max_string_size};
};

StringSet waitOrAllocate(notifying_queue::Queue<StringSet, false>& empty_queue, std::size_t& blocks_in_memory,
                         const std::size_t max_blocks_in_memory, const std::size_t size)
{
  StringSet set;
  if (blocks_in_memory < max_blocks_in_memory)
  {
    if (!empty_queue.tryPop(set))
    {
      set.allocate(size);
      ++blocks_in_memory;
    }

    set.clear();
    return set;
  }

  empty_queue.waitAndPop(set);
  set.clear();
  return set;
}

void finalize(std::ifstream& file, const char* const str, std::ofstream& target)
{
  target << str << '\n';

  for (;;)
  {
    char tmp[max_string_size];

    file.getline(tmp, sizeof(tmp));
    if (file.eof())
    {
      break;
    }
    target << tmp << '\n';
  }
}

void merge(std::ifstream& file1, std::ifstream& file2, std::ofstream& target)
{
  char str1[max_string_size];
  char str2[max_string_size];
  
  file1.getline(str1, sizeof(str1));
  file2.getline(str2, sizeof(str2));
  for (;;)
  {
    if (strcmp(str1, str2) < 0)
    {
      target << str1 << '\n';
      file1.getline(str1, sizeof(str1));
      if (file1.eof())
      {
        finalize(file2, str2, target);
	return;
      }
    }
    else
    {
      target << str2 << '\n';
      file2.getline(str2, sizeof(str2));
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
    files_queue.pushForce(std::move(new_file_name));

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
	StringSet portion;
        if (!strings_buffers.waitAndPop(portion))
        {
          break;
        }

        const std::size_t file_index = files_enumerator.fetch_add(1, std::memory_order_relaxed);

        std::string filename{"tmp" + std::to_string(file_index)};

        {
          std::vector<char*> to_sort;
	  to_sort.reserve(strs_by_thread);
	  for (std::size_t i{}; i < strs_by_thread; ++i)
	  {
            if (portion[i][0] == '\0')
	    {
              break;
	    }
            to_sort.push_back(portion[i]);
	  }

          std::ofstream file{filename};

          std::sort(std::begin(to_sort), std::end(to_sort), 
			  [](const char* const str1, const char* const str2) noexcept { return strcmp(str1, str2) < 0; });

          for (const auto &str : to_sort)
          {
            file << str << '\n';
          }
	  ready_buffers.push(std::move(portion));
        }
        file_names.pushForce(std::move(filename));
      }
    }

    processReduce(file_names, files_enumerator, num_of_remaining_files, order_num);
  }
  std::atomic_size_t& num_of_remaining_files;
  notifying_queue::Queue<StringSet, false>& strings_buffers;
  notifying_queue::Queue<StringSet, false>& ready_buffers;
  notifying_queue::DoublePopQueue<std::string>& file_names;
  std::atomic_size_t& files_enumerator;

  const std::size_t strs_by_thread;

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

  notifying_queue::Queue<StringSet, false> strings_buffers;
  notifying_queue::Queue<StringSet, false> ready_buffers;
  std::size_t blocks_in_memory{0};

  std::atomic_size_t num_of_remaining_files{0};
  std::atomic_size_t files_enumerator{0};

  notifying_queue::DoublePopQueue<std::string> file_names{files_queue_capacity};

  for (std::size_t i{}; i < num_of_threads_to_create; ++i)
  {
    ThreadAction action{num_of_remaining_files,
                        strings_buffers, ready_buffers, file_names,
                        files_enumerator, strings_by_thread, i + 1};

    std::thread t{action};
    t.detach();
  }

  {
    auto portion = waitOrAllocate(ready_buffers, blocks_in_memory, num_of_threads, strings_by_thread);

    int cnt{};

    for (;;)
    {
      file.getline(portion[cnt++], StringSet::str_buffer_size);
      if (file.eof())
      {
        portion[--cnt][0] = '\0';

        if (cnt)
        {
          num_of_remaining_files.fetch_add(1, std::memory_order_relaxed);
          strings_buffers.push(std::move(portion));
        }
        break;
      }

      if (cnt == strings_by_thread)
      {
        num_of_remaining_files.fetch_add(1, std::memory_order_relaxed);
        strings_buffers.push(std::move(portion));
        portion = waitOrAllocate(ready_buffers, blocks_in_memory, num_of_threads,
                                 strings_by_thread);
	cnt = 0;
      }
    }

    strings_buffers.finish();
    ready_buffers.finish();
  }

  processReduce<true>(file_names, files_enumerator, num_of_remaining_files, 0);

  return 0;
}
