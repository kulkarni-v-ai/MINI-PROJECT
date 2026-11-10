# ============================================================================
# Resilient Multi-Peer File Distribution & Recovery Engine - Gate 1
# C++17 / CMake / OpenSSL (SHA-256 only) / std::thread / TCP sockets
#
# Builds natively on Windows 11 (Visual Studio 2022 / MSVC) and on Linux.
#   cmake -S . -B build
#   cmake --build build --config Release
# ============================================================================
cmake_minimum_required(VERSION 3.16)
project(resilient_file_engine CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Single-config generators (Makefiles/Ninja) default to Release; multi-config
# generators (Visual Studio) choose with --config.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(MSVC)
  # /utf-8: sources and narrow string literals are UTF-8 (paths are handled
  # as UTF-8 throughout and converted to UTF-16 at the Win32 boundary).
  add_compile_options(/W4 /utf-8 /permissive-)
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS _WINSOCK_DEPRECATED_NO_WARNINGS)
else()
  add_compile_options(-Wall -Wextra)
endif()

if(WIN32)
  # Windows 10 / 11 API level; keep <windows.h> from defining min()/max() or
  # pulling in the legacy winsock.h.
  add_compile_definitions(_WIN32_WINNT=0x0A00 WINVER=0x0A00 NOMINMAX WIN32_LEAN_AND_MEAN UNICODE _UNICODE)
endif()

if(WIN32 AND NOT OPENSSL_ROOT_DIR AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/openssl/include/openssl/evp.h")
  set(OPENSSL_ROOT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/openssl")
endif()

find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

# Platform-specific process control is isolated from E1-E6. The common file
# (pid files, project-root discovery) is shared by both implementations.
if(WIN32)
  set(PROCESS_PLATFORM_SOURCE engine/process/process_manager_windows.cpp)
else()
  set(PROCESS_PLATFORM_SOURCE engine/process/process_manager_linux.cpp)
endif()

add_library(engine STATIC
  engine/chunking/chunk_manager.cpp
  engine/communication/protocol.cpp
  engine/communication/tcp_server.cpp
  engine/communication/tcp_client.cpp
  engine/distribution/distributor.cpp
  engine/failure_detection/failure_detector.cpp
  engine/recovery/recovery_manager.cpp
  engine/metadata/metadata_manager.cpp
  engine/registry/peer_registry.cpp
  engine/coordinator/coordinator_service.cpp
  engine/coordinator/http_server.cpp
  engine/session/session_manager.cpp
  engine/coding/gf256.cpp
  engine/coding/reed_solomon.cpp
  engine/coding/adaptive_chunker.cpp
  engine/catalog/catalog_manager.cpp
  engine/distribution/dynamic_distributor.cpp
  engine/recovery/client_download.cpp
  engine/process/process_manager_common.cpp
  ${PROCESS_PLATFORM_SOURCE}
)
target_include_directories(engine PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(engine PUBLIC OpenSSL::Crypto Threads::Threads)

if(WIN32)
  # ws2_32: sockets; shell32: CommandLineToArgvW (UTF-8 command line);
  # advapi32/crypt32/user32: needed when OpenSSL is a static library;
  # bcrypt: BCryptGenRandom CSPRNG for session tokens.
  target_link_libraries(engine PUBLIC ws2_32 shell32 advapi32 crypt32 user32 bcrypt)
endif()

add_executable(peer peer/peer.cpp)
target_link_libraries(peer PRIVATE engine)

add_executable(controller controller/controller.cpp)
target_link_libraries(controller PRIVATE engine)

add_executable(rfe cli/rfe.cpp)
target_link_libraries(rfe PRIVATE engine)

# ---- Windows: make the OpenSSL DLLs available next to the executables --------
# (vcpkg's toolchain already does this itself; this covers a plain OpenSSL
#  installation such as C:\Program Files\OpenSSL-Win64.)
if(WIN32)
  set(_ossl_bin_dirs "")
  if(OPENSSL_ROOT_DIR)
    list(APPEND _ossl_bin_dirs "${OPENSSL_ROOT_DIR}/bin" "${OPENSSL_ROOT_DIR}")
  endif()
  if(OPENSSL_INCLUDE_DIR)
    get_filename_component(_ossl_prefix "${OPENSSL_INCLUDE_DIR}" DIRECTORY)
    list(APPEND _ossl_bin_dirs "${_ossl_prefix}/bin")
  endif()
  set(_ossl_dlls "")
  foreach(_dir IN LISTS _ossl_bin_dirs)
    file(GLOB _found "${_dir}/libcrypto*.dll")
    list(APPEND _ossl_dlls ${_found})
  endforeach()
  list(REMOVE_DUPLICATES _ossl_dlls)
  if(_ossl_dlls)
    foreach(_tgt peer controller rfe)
      add_custom_command(TARGET ${_tgt} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different ${_ossl_dlls} $<TARGET_FILE_DIR:${_tgt}>
        COMMENT "Copying OpenSSL runtime DLLs next to ${_tgt}")
    endforeach()
  endif()
endif()

# ---- Tests: Gate-1 T01..T07 (run from the project root) ----------------------
enable_testing()
add_test(NAME gate1
  COMMAND controller test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
