# Copyright (c) 2026 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

cmake_minimum_required(VERSION 3.19)

project(taosgen_ingress_cpp_client_example LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(Threads REQUIRED)

if(TARGET protobuf::libprotobuf)
  set(TAOSGEN_EXAMPLE_PROTOBUF_LIB protobuf::libprotobuf)
elseif(TARGET protobuf::protobuf)
  set(TAOSGEN_EXAMPLE_PROTOBUF_LIB protobuf::protobuf)
elseif(TARGET Protobuf::libprotobuf)
  set(TAOSGEN_EXAMPLE_PROTOBUF_LIB Protobuf::libprotobuf)
else()
  message(FATAL_ERROR "Could not find protobuf library target")
endif()

if(TARGET protobuf::protoc)
  set(TAOSGEN_EXAMPLE_PROTOC_EXE $<TARGET_FILE:protobuf::protoc>)
elseif(TARGET Protobuf::protoc)
  set(TAOSGEN_EXAMPLE_PROTOC_EXE $<TARGET_FILE:Protobuf::protoc>)
elseif(DEFINED Protobuf_PROTOC_EXECUTABLE)
  set(TAOSGEN_EXAMPLE_PROTOC_EXE ${Protobuf_PROTOC_EXECUTABLE})
else()
  message(FATAL_ERROR "Could not find protoc executable")
endif()

if(TARGET gRPC::grpc_cpp_plugin)
  set(TAOSGEN_EXAMPLE_GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
  message(FATAL_ERROR "Could not find grpc_cpp_plugin target")
endif()

get_filename_component(TAOSGEN_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)
set(TAOSGEN_INGRESS_PROTO_FILE
  "${TAOSGEN_ROOT}/proto/taosgen/ingress/ingestion.proto"
)
set(TAOSGEN_INGRESS_PROTO_GEN_DIR
  "${CMAKE_CURRENT_BINARY_DIR}/generated"
)
set(TAOSGEN_INGRESS_PROTO_SRCS
  "${TAOSGEN_INGRESS_PROTO_GEN_DIR}/taosgen/ingress/ingestion.pb.cc"
  "${TAOSGEN_INGRESS_PROTO_GEN_DIR}/taosgen/ingress/ingestion.grpc.pb.cc"
)
set(TAOSGEN_INGRESS_PROTO_HDRS
  "${TAOSGEN_INGRESS_PROTO_GEN_DIR}/taosgen/ingress/ingestion.pb.h"
  "${TAOSGEN_INGRESS_PROTO_GEN_DIR}/taosgen/ingress/ingestion.grpc.pb.h"
)

add_custom_command(
  OUTPUT
    ${TAOSGEN_INGRESS_PROTO_SRCS}
    ${TAOSGEN_INGRESS_PROTO_HDRS}
  COMMAND ${CMAKE_COMMAND} -E make_directory "${TAOSGEN_INGRESS_PROTO_GEN_DIR}"
  COMMAND ${TAOSGEN_EXAMPLE_PROTOC_EXE}
    --proto_path=${TAOSGEN_ROOT}/proto
    --cpp_out=${TAOSGEN_INGRESS_PROTO_GEN_DIR}
    --grpc_out=${TAOSGEN_INGRESS_PROTO_GEN_DIR}
    --plugin=protoc-gen-grpc=${TAOSGEN_EXAMPLE_GRPC_CPP_PLUGIN}
    ${TAOSGEN_INGRESS_PROTO_FILE}
  DEPENDS "${TAOSGEN_INGRESS_PROTO_FILE}"
  VERBATIM
)

add_library(taosgen_ingress_example_proto STATIC
  ${TAOSGEN_INGRESS_PROTO_SRCS}
)

target_include_directories(taosgen_ingress_example_proto
  PUBLIC
    "${TAOSGEN_INGRESS_PROTO_GEN_DIR}"
)

target_link_libraries(taosgen_ingress_example_proto
  PUBLIC
    ${TAOSGEN_EXAMPLE_PROTOBUF_LIB}
    gRPC::grpc++
)

if(NOT MSVC)
  target_compile_options(taosgen_ingress_example_proto PRIVATE -w)
endif()

add_executable(taosgen_ingress_cpp_client
  ingress_cpp_client.cpp
)

target_link_libraries(taosgen_ingress_cpp_client
  PRIVATE
    taosgen_ingress_example_proto
    gRPC::grpc++
    Threads::Threads
)

if(NOT MSVC)
  target_compile_options(taosgen_ingress_cpp_client PRIVATE -Wall -Wextra -Werror -O3 -g)
endif()
