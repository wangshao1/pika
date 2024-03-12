cmake_minimum_required(VERSION 3.18)

include(GoogleTest)

file(GLOB_RECURSE PIKA_TEST_SOURCE "${CMAKE_SOURCE_DIR}/tests/gtest/*.cc")

aux_source_directory(../../src TEST_SRCS)

add_compile_definitions(PIKA_ROOT_DIR="${CMAKE_SOURCE_DIR}")

foreach(pika_test_source ${PIKA_TEST_SOURCE})
    get_filename_component(pika_test_filename ${pika_test_source} NAME)
    string(REPLACE ".cc" "" pika_test_name ${pika_test_filename})

    # Add the test target
    add_executable(${pika_test_name} ${pika_test_source})
    target_include_directories(${pika_test_name}
            PUBLIC ${CMAKE_SOURCE_DIR}/include
            PUBLIC ${PROJECT_SOURCE_DIR}/include
            PUBLIC ${PROJECT_SOURCE_DIR}/..
            ${ROCKSDB_INCLUDE_DIR}
            ${ROCKSDB_SOURCE_DIR}
    )
    add_dependencies(${pika_test_name} gtest glog gflags ${LIBUNWIND_NAME})
    target_link_libraries(${pika_test_name}
            PUBLIC ${GTEST_LIBRARY}
            PUBLIC ${ROCKSDB_LIBRARY}
            PUBLIC pstd
            PUBLIC net
            PUBLIC storage
            PUBLIC ${GLOG_LIBRARY}
            PUBLIC ${GFLAGS_LIBRARY}
            PUBLIC ${LIBUNWIND_LIBRARY}
    )
    if (USE_S3)
        target_link_libraries(${pika_test_name}
                PUBLIC libaws-cpp-sdk-core.so
                PUBLIC libaws-cpp-sdk-transfer.so
                PUBLIC libaws-cpp-sdk-kinesis.so
                PUBLIC libaws-cpp-sdk-s3.so
        )
    endif()
    add_test(NAME ${pika_test_name}
            COMMAND ${pika_test_name}
            WORKING_DIRECTORY .)
endforeach(pika_test_source ${PIKA_TEST_SOURCE})
