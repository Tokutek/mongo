#!/bin/bash

a=$1
b=$2

sed -i "s/$a/$b/g" *.cpp *.h */*.cpp */*.h
