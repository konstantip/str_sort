#!/bin/bash

cd generator && ./build.sh && mv generate .. && cd ..
cd checker && ./build.sh && mv check .. && cd ..
cd sorter && ./build.sh && mv sort .. && cd ..
