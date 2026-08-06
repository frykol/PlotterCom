cmake_minimum_required(VERSION 3.29)

project(PlotterCom)

add_executable(PlotterCom
              src/main.c
              src/communication/message.c
              src/communication/message_queue.c)

target_include_directories(PlotterCom
            PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include)

