#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Note: This script depends on 5 environment variables to function correctly:
# a) CLASSPATH
# b) HADOOP_HOME
# c) HADOOP_CONF_DIR 
# d) HADOOP_LOG_DIR 
# e) LIBHDFS_BUILD_DIR
# f) LIBHDFS_INSTALL_DIR
# g) OS_NAME
# All these are passed by build.xml.
#

HDFS_TEST=hdfs_test
HADOOP_LIB_DIR=$HADOOP_HOME/lib
HADOOP_BIN_DIR=$HADOOP_HOME/bin

COMMON_BUILD_DIR=$HADOOP_HOME/build/ivy/lib/Hadoop-Hdfs/common
COMMON_JAR=$COMMON_BUILD_DIR/hadoop-core-0.22.0-SNAPSHOT.jar

# If we are running from the hdfs repo we need to create HADOOP_BIN_DIR.  
# If the bin directory does not and we've got a core jar extract it's
# bin directory to HADOOP_HOME/bin. The bin scripts hdfs-config.sh and
# hadoop-config.sh assume the bin directory is named "bin" and that it
# is located in HADOOP_HOME.
created_bin_dir=0
if [ ! -d $HADOOP_BIN_DIR ]; then
  if [ -f $COMMON_JAR ]; then
    mkdir $HADOOP_BIN_DIR
    jar xf $COMMON_JAR bin.tgz
    tar xfz bin.tgz -C $HADOOP_BIN_DIR
    created_bin_dir=1
  fi
fi

# Manipulate HADOOP_CONF_DIR too
# which is necessary to circumvent bin/hadoop
HADOOP_CONF_DIR=$HADOOP_CONF_DIR:$HADOOP_HOME/conf

# set pid file dir so they are not written to /tmp
export HADOOP_PID_DIR=$HADOOP_LOG_DIR

# CLASSPATH initially contains $HADOOP_CONF_DIR
CLASSPATH="${HADOOP_CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

# for developers, add Hadoop classes to CLASSPATH
if [ -d "$HADOOP_HOME/build/classes" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_HOME/build/classes
fi
if [ -d "$HADOOP_HOME/build/webapps" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_HOME/build
fi
if [ -d "$HADOOP_HOME/build/test/classes" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_HOME/build/test/classes
fi

# so that filenames w/ spaces are handled correctly in loops below
IFS=

# add libs to CLASSPATH
for f in $HADOOP_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

for f in $HADOOP_HOME/*.jar; do 
  CLASSPATH=${CLASSPATH}:$f
done
for f in $HADOOP_HOME/lib/jsp-2.1/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

if [ -d "$COMMON_BUILD_DIR" ]; then
  CLASSPATH=$CLASSPATH:$COMMON_JAR
  for f in $COMMON_BUILD_DIR/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
  done
fi

# restore ordinary behaviour
unset IFS

findlibjvm () {
javabasedir=$JAVA_HOME
case $OS_NAME in
    cygwin* | mingw* | pw23* )
    lib_jvm_dir=`find $javabasedir -follow \( \
        \( -name client -type d -prune \) -o \
        \( -name "jvm.dll" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    ;;
    aix*)
    lib_jvm_dir=`find $javabasedir \( \
        \( -name client -type d -prune \) -o \
        \( -name "libjvm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    if test -z "$lib_jvm_dir"; then
       lib_jvm_dir=`find $javabasedir \( \
       \( -name client -type d -prune \) -o \
       \( -name "libkaffevm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    fi
    ;;
    *)
    lib_jvm_dir=`find $javabasedir -follow \( \
       \( -name client -type d -prune \) -o \
       \( -name "libjvm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    if test -z "$lib_jvm_dir"; then
       lib_jvm_dir=`find $javabasedir -follow \( \
       \( -name client -type d -prune \) -o \
       \( -name "libkaffevm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    fi
    ;;
  esac
  echo $lib_jvm_dir
}
LIB_JVM_DIR=`findlibjvm`
echo  "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo  LIB_JVM_DIR = $LIB_JVM_DIR
echo  "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
# Put delays to ensure hdfs is up and running and also shuts down 
# after the tests are complete
cd $HADOOP_HOME
echo Y | $HADOOP_BIN_DIR/hdfs namenode -format &&
$HADOOP_BIN_DIR/hadoop-daemon.sh --script $HADOOP_BIN_DIR/hdfs start namenode && sleep 2
$HADOOP_BIN_DIR/hadoop-daemon.sh --script $HADOOP_BIN_DIR/hdfs start datanode && sleep 2
sleep 20
CLASSPATH=$CLASSPATH LD_PRELOAD="$LIB_JVM_DIR/libjvm.so:$LIBHDFS_INSTALL_DIR/libhdfs.so:" $LIBHDFS_BUILD_DIR/$HDFS_TEST
BUILD_STATUS=$?
sleep 3
$HADOOP_BIN_DIR/hadoop-daemon.sh --script $HADOOP_BIN_DIR/hdfs stop datanode && sleep 2
$HADOOP_BIN_DIR/hadoop-daemon.sh --script $HADOOP_BIN_DIR/hdfs stop namenode && sleep 2 

if [ $created_bin_dir -eq 1 ]; then
  rm -rf bin.tgz $HADOOP_BIN_DIR 
fi

echo exiting with $BUILD_STATUS
exit $BUILD_STATUS