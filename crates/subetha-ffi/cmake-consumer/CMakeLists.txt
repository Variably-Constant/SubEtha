cmake_minimum_required(VERSION 3.16)
project(subetha_consumer C)

# A C program taking SubEtha through the installed package, both ways: the
# shared library a program loads at run time, and the static library linked
# into the executable. `cargo xtask ffi-install --prefix <dir>` produces the
# tree find_package reads; point CMAKE_PREFIX_PATH at that directory.
find_package(subetha REQUIRED)

add_executable(subetha_consumer_shared main.c)
target_link_libraries(subetha_consumer_shared PRIVATE subetha::subetha)

add_executable(subetha_consumer_static main.c)
target_link_libraries(subetha_consumer_static PRIVATE subetha::static)
