function(build_wiredtiger)
  set(wiredtiger_CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON)

  # cmake doesn't properly handle arguments containing ";", such as
  # CMAKE_PREFIX_PATH, for which reason we'll have to use some other separator.
  #string(REPLACE ";" "!" CMAKE_PREFIX_PATH_ALT_SEP "${CMAKE_PREFIX_PATH}")
  #list(APPEND wiredtiger_CMAKE_ARGS -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH_ALT_SEP})
  if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND wiredtiger_CMAKE_ARGS
         -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE})
  endif()

  list(APPEND wiredtiger_CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})

  list(APPEND wiredtiger_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
  list(APPEND wiredtiger_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})
  list(APPEND wiredtiger_CMAKE_ARGS -DENABLE_STATIC=1)
  CHECK_C_COMPILER_FLAG("-Wno-stringop-truncation" HAS_WARNING_STRINGOP_TRUNCATION)
  if(HAS_WARNING_STRINGOP_TRUNCATION)
    list(APPEND wiredtiger_CMAKE_ARGS -DCMAKE_C_FLAGS=-Wno-stringop-truncation)
  endif()
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag("-Wno-deprecated-copy" HAS_WARNING_DEPRECATED_COPY)
  if(HAS_WARNING_DEPRECATED_COPY)
    set(wiredtiger_CXX_FLAGS -Wno-deprecated-copy)
  endif()
  check_cxx_compiler_flag("-Wno-pessimizing-move" HAS_WARNING_PESSIMIZING_MOVE)
  if(HAS_WARNING_PESSIMIZING_MOVE)
    set(wiredtiger_CXX_FLAGS "${wiredtiger_CXX_FLAGS} -Wno-pessimizing-move")
  endif()
  if(wiredtiger_CXX_FLAGS)
    list(APPEND wiredtiger_CMAKE_ARGS -DCMAKE_CXX_FLAGS='${wiredtiger_CXX_FLAGS}')
  endif()
  # we use an external project and copy the sources to bin directory to ensure
  # that object files are built outside of the source tree.
  include(ExternalProject)
  set(wiredtiger_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/wiredtiger")
  set(wiredtiger_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/wiredtiger")
  set(wiredtiger_LIBRARY "${wiredtiger_BINARY_DIR}/libwiredtiger.a")

  if(CMAKE_MAKE_PROGRAM MATCHES "make")
    # try to inherit command line arguments passed by parent "make" job
    set(make_cmd $(MAKE))
  else()
    set(make_cmd ${CMAKE_COMMAND} --build <BINARY_DIR> )
  endif()

  ExternalProject_Add(wiredtiger_ext
    SOURCE_DIR "${wiredtiger_SOURCE_DIR}"
    CMAKE_ARGS ${wiredtiger_CMAKE_ARGS}
    BINARY_DIR "${wiredtiger_BINARY_DIR}"
    BUILD_COMMAND "${make_cmd}"
    BUILD_BYPRODUCTS "${wiredtiger_LIBRARY}"
    INSTALL_COMMAND ""
    LIST_SEPARATOR !)

  add_library(WiredTiger::WiredTiger STATIC IMPORTED)
  add_dependencies(WiredTiger::WiredTiger wiredtiger_ext)
  set(wiredtiger_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/wiredtiger/include")
  set_target_properties(WiredTiger::WiredTiger PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${wiredtiger_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES "${wiredtiger_INTERFACE_LINK_LIBRARIES}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION "${wiredtiger_LIBRARY}"
    VERSION "11.1.0")
endfunction()
