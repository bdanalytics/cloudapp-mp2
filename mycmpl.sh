#!/bin/bash
rm -rf ./build/$1*.class ./$1.jar
hadoop com.sun.tools.javac.Main $1.java -d build
jar -cvf $1.jar -C build/ ./
