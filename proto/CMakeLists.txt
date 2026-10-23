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

find_package(protobuf REQUIRED CONFIG)
find_package(gRPC REQUIRED CONFIG)

if(TARGET protobuf::libprotobuf)
  set(TSGEN_PROTOBUF_LIB protobuf::libprotobuf)
elseif(TARGET protobuf::protobuf)
  set(TSGEN_PROTOBUF_LIB protobuf::protobuf)
elseif(TARGET Protobuf::libprotobuf)
  set(TSGEN_PROTOBUF_LIB Protobuf::libprotobuf)
else()
  message(FATAL_ERROR "Could not find protobuf library target")
endif()

set(TSGEN_PROTOBUF_PACKAGE_DIR "")
if(DEFINED protobuf_PACKAGE_FOLDER_RELEASE)
  set(TSGEN_PROTOBUF_PACKAGE_DIR "${protobuf_PACKAGE_FOLDER_RELEASE}")
elseif(DEFINED protobuf_PACKAGE_FOLDER_DEBUG)
  set(TSGEN_PROTOBUF_PACKAGE_DIR "${protobuf_PACKAGE_FOLDER_DEBUG}")
elseif(DEFINED protobuf_PACKAGE_FOLDER)
  set(TSGEN_PROTOBUF_PACKAGE_DIR "${protobuf_PACKAGE_FOLDER}")
endif()

if(NOT TSGEN_PROTOBUF_PACKAGE_DIR)
  message(FATAL_ERROR "Could not find protobuf package folder")
endif()

set(TSGEN_PROTOC_EXE "${TSGEN_PROTOBUF_PACKAGE_DIR}/bin/protoc${CMAKE_EXECUTABLE_SUFFIX}")

if(NOT EXISTS "${TSGEN_PROTOC_EXE}")
  message(FATAL_ERROR "Could not find protoc executable in protobuf package: ${TSGEN_PROTOC_EXE}")
endif()

if(TARGET protobuf::protoc)
  set_target_properties(protobuf::protoc PROPERTIES IMPORTED_LOCATION "${TSGEN_PROTOC_EXE}")
elseif(TARGET Protobuf::protoc)
  set_target_properties(Protobuf::protoc PROPERTIES IMPORTED_LOCATION "${TSGEN_PROTOC_EXE}")
endif()

set(Protobuf_PROTOC_EXECUTABLE "${TSGEN_PROTOC_EXE}")

if(TARGET gRPC::grpc_cpp_plugin)
  set(TSGEN_GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
  message(FATAL_ERROR "Could not find grpc_cpp_plugin target")
endif()

set(TSGEN_INGRESS_PROTO_FILE
  ${CMAKE_CURRENT_SOURCE_DIR}/taosgen/ingress/ingestion.proto
)
set(TSGEN_INGRESS_PROTO_GEN_DIR
  ${CMAKE_CURRENT_BINARY_DIR}/generated
)
set(TSGEN_INGRESS_PROTO_SRCS
  ${TSGEN_INGRESS_PROTO_GEN_DIR}/taosgen/ingress/ingestion.pb.cc
  ${TSGEN_INGRESS_PROTO_GEN_DIR}/taosgen/ingress/ingestion.grpc.pb.cc
)
set(TSGEN_INGRESS_PROTO_HDRS
  ${TSGEN_INGRESS_PROTO_GEN_DIR}/taosgen/ingress/ingestion.pb.h
  ${TSGEN_INGRESS_PROTO_GEN_DIR}/taosgen/ingress/ingestion.grpc.pb.h
)

add_custom_command(
  OUTPUT
    ${TSGEN_INGRESS_PROTO_SRCS}
    ${TSGEN_INGRESS_PROTO_HDRS}
  COMMAND ${CMAKE_COMMAND} -E make_directory ${TSGEN_INGRESS_PROTO_GEN_DIR}
  COMMAND ${TSGEN_PROTOC_EXE}
    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
    --cpp_out=${TSGEN_INGRESS_PROTO_GEN_DIR}
    --grpc_out=${TSGEN_INGRESS_PROTO_GEN_DIR}
    --plugin=protoc-gen-grpc=${TSGEN_GRPC_CPP_PLUGIN}
    ${TSGEN_INGRESS_PROTO_FILE}
  DEPENDS ${TSGEN_INGRESS_PROTO_FILE}
  VERBATIM
)

add_library(taosgen_ingress_proto STATIC
  ${TSGEN_INGRESS_PROTO_SRCS}
)

target_include_directories(taosgen_ingress_proto
  PUBLIC
    ${TSGEN_INGRESS_PROTO_GEN_DIR}
)

target_link_libraries(taosgen_ingress_proto
  PUBLIC
    ${TSGEN_PROTOBUF_LIB}
    gRPC::grpc++
)

if(NOT MSVC)
  target_compile_options(taosgen_ingress_proto PRIVATE -w)
endif()
