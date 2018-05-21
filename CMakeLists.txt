cmake_minimum_required (VERSION 2.8)

include(CheckIncludeFile)
include(FindPackageHandleStandardArgs)
include(TestCXXAcceptsFlag)

project(cpp-driver-bench)

# Gather SIGAR - System Information Gatherer And Reporter source files
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules")
include(sigar)
GatherSIGAR(src/third_party/sigar/src)
if(WIN32)
  target_link_libraries(cpp-driver-bench ws2_32 netapi32 version)
endif()
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-result")

file(GLOB SRC_FILES "src/*.h" "src/*.hpp" "src/*.cpp")
add_executable(cpp-driver-bench ${SRC_FILES} ${SIGAR_SRC})

target_compile_options(cpp-driver-bench PRIVATE "-Wall")
target_compile_options(cpp-driver-bench PRIVATE "-Wno-deprecated-declarations")

# Find libuv
find_path(LIBUV_INCLUDE_DIR
	NAMES uv.h
	HINTS ${_LIBUV_INCLUDEDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
	PATH_SUFFIXES include)
find_library(LIBUV_LIBRARY
	NAMES uv libuv
	HINTS ${_LIBUV_LIBDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
	PATH_SUFFIXES lib)
find_package_handle_standard_args(Libuv "Could NOT find libuv, try to set the path to the libuv root folder in the system variable LIBUV_ROOT_DIR"
	LIBUV_LIBRARY
	LIBUV_INCLUDE_DIR)

include_directories(${LIBUV_INCLUDE_DIR})
target_link_libraries(cpp-driver-bench ${LIBUV_LIBRARY})

# Ensure C++11 is available
check_cxx_accepts_flag("-std=c++11" HAVE_CXX11)
if(NOT HAVE_CXX11)
  message(FATAL_ERROR "C++11 is required")
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# Fix name mangling for SIGAR usage
check_cxx_accepts_flag("-fgnu89-inline" HAVE_FGNU89_INLINE)
if(HAVE_FGNU89_INLINE)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fgnu89-inline")
endif()

# Find a driver
find_path(DSE_DRIVER_INCLUDE_DIR
  NAMES dse.h
  HINTS ../ ../../ ../../../
  PATHS ${DSE_DRIVER_ROOT} ENV DSE_DRIVER_ROOT
  PATH_SUFFIXES include)

if(NOT DSE_DRIVER_INCLUDE_DIR)
  message(WARNING "Unable to find 'dse.h' trying 'cassandra.h'")

  find_path(CASS_DRIVER_INCLUDE_DIR
    NAMES cassandra.h
    HINTS ../ ../../ ../../../
    PATHS ${CASS_DRIVER_ROOT} ENV CASS_DRIVER_ROOT
    PATH_SUFFIXES include)

  if(NOT CASS_DRIVER_INCLUDE_DIR)
    message(FATAL_ERROR "Unable to find any driver header file")
  endif()

  list(APPEND CMAKE_REQUIRED_INCLUDES ${CASS_DRIVER_INCLUDE_DIR})
  include_directories(${CASS_DRIVER_INCLUDE_DIR})

  find_library(CASS_DRIVER_LIBRARY
    NAMES libcassandra cassandra
    HINTS ../ ../../ ../../../
    PATHS ${CASS_DRIVER_ROOT} ENV CASS_DRIVER_ROOT
    PATH_SUFFIXES lib)

  if(NOT CASS_DRIVER_LIBRARY)
    message(FATAL_ERROR "Unable to find cassandra library")
  endif()

  target_link_libraries(cpp-driver-bench ${CASS_DRIVER_LIBRARY})
else()
  list(APPEND CMAKE_REQUIRED_INCLUDES ${DSE_DRIVER_INCLUDE_DIR})
  include_directories(${DSE_DRIVER_INCLUDE_DIR})

  find_library(DSE_DRIVER_LIBRARY
    NAMES libdse dse
    HINTS ../ ../../ ../../../
    PATHS ${DSE_DRIVER_ROOT} ENV DSE_DRIVER_ROOT
    PATH_SUFFIXES lib)

  if(NOT DSE_DRIVER_LIBRARY)
    message(FATAL_ERROR "Unable to find dse library")
  endif()

  target_link_libraries(cpp-driver-bench ${DSE_DRIVER_LIBRARY})
endif()

check_include_file(dse.h HAVE_DSE_H)
check_include_file(cassandra.h HAVE_CASSANDRA_H)

configure_file("src/benchconfig.hpp.cmake" "benchconfig.hpp")
include_directories(${CMAKE_CURRENT_BINARY_DIR})
