#include <fstream>
#include <iostream>
#include <unordered_set>

int main(const int argc, const char* const argv[])
{
  if (argc < 3)
  {
    std::cerr << "File names is needed!!!" << std::endl;
    return -1;
  }

  std::ifstream result{argv[1]};

  if (result.fail())
  {
    std::cerr << "Fail to open file " << argv[1] << std::endl;
    return -2;
  }

  std::ifstream source{argv[2]};
  if (source.fail())
  {
    std::cerr << "Fail to open file " << argv[1] << std::endl;
    return -2;
  }


  std::unordered_multiset<std::string> map;

  {
    std::string prev;
    result >> prev;
    if (!result.eof())
    {
      for (;;)
      {
        map.emplace(prev);
        std::string current;
        result >> current;
        if (result.eof())
        {
          break;
        }
        if (current < prev)
        {
          return 1;
        }
        prev = std::move(current);
      }
    }
  }


  {
    std::unordered_multiset<std::string> missed;

    for (;;)
    {
      std::string current;
      source >> current;
      if (source.eof())
      {
        break;
      }

      const auto it = map.find(current);

      if (it == map.cend())
      {
        missed.insert(std::move(current));
	continue;
      }

      map.erase(it);
    }

    if (map.empty() && missed.empty())
    {
      return 0;
    }

    std::ofstream fails{"diff"};
    
    if (!missed.empty())
    {
      fails << "Missed strings: \n";

      for (const auto& el : missed)
      {
        fails << el << "\n";
      }
    }
    if (!map.empty())
    {
      fails << "additional strings: \n";

      for (const auto& el : map)
      {
        fails << el << "\n";
      }
    }

    return 2;
  }


  return 0;
}
