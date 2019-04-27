
find_library(ARROW_LIBRARIES
        NAMES libarrow
        HINTS /usr/local/lib
        )

#find_library(ARROW_FLIGHT_LIBRARIES
#        NAMES libarrow_flight
#        HINTS /usr/local/lib
#        )

find_path(ARROW_INCLUDE_DIR
        NAMES arrow/api.h
        HINTS /usr/local/include
        )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Arrow DEFAULT_MSG
        ARROW_LIBRARIES
        ARROW_FLIGHT_LIBRARIES
        ARROW_INCLUDE_DIR
        )

mark_as_advanced(
        ARROW_LIBRARIES
        ARROW_FLIGHT_LIBRARIES
        ARROW_INCLUDE_DIR
)

message(STATUS "Found arrow (include: ${ARROW_INCLUDE_DIRS}, library: ${ARROW_LIBRARIES})")
#message(STATUS "Found arrow flight (include: ${ARROW_INCLUDE_DIRS}, library: ${ARROW_FLIGHT_LIBRARIES})")