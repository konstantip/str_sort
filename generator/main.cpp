#include <fstream>
#include <iostream>
#include <ctime>

constexpr std::size_t max_str_size{10};

int main(int argc, char* argv[])
{
  if (argc < 2)
  {
    std::cerr << "Invalid argument, array size is needed" << std::endl;
    std::terminate();
  }
  int len{};
  if (!(len = std::stoi(argv[1]))) {
    std::cerr << "Incorrect size of array" << std::endl;

    std::terminate();
  }

  std::ofstream file{"str_array.txt"};
  std::srand(std::time(0));

  for (int i = 0; i < len; ++i)
  {
    const std::size_t str_size = std::rand() % (max_str_size - 1) + 1;

    for (int j = 0; j < str_size; ++j)
    {
      file << char(std::rand() % (90 - 65) + 65);
    }
    file << '\n';
  }
  return 0;
}
