
find_library(ARROW_LIBRARIES
        NAMES libarrow.a
        HINTS /usr/local/lib
        )

find_path(ARROW_INCLUDE_DIR
        NAMES arrow/api.h
        HINTS /usr/local/include
        )

find_library(ARROW_PYTHON
        NAMES libarrow_python.a
        HINTS /usr/local/lib
        )

find_library(DOUBLE_CONVERSION
        NAMES libdouble-conversion.a
        HINTS /usr/local/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Arrow DEFAULT_MSG
        ARROW_LIBRARIES
        ARROW_INCLUDE_DIR
        ARROW_PYTHON
        DOUBLE_CONVERSION
        )

mark_as_advanced(
        ARROW_LIBRARIES
        ARROW_INCLUDE_DIR
        ARROW_PYTHON
        DOUBLE_CONVERSION
)

message(STATUS "Found arrow (include: ${ARROW_INCLUDE_DIRS}, library: ${ARROW_LIBRARIES})")