#!/bin/bash

./generate $1

./sort str_array.txt

./check result str_array\.txt

echo $?
