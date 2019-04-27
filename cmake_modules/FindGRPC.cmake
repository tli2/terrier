find_library(GRPC_LIBRARIES
        NAMES libgrpc
        HINTS /usr/local/lib
        )


include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GRPC DEFAULT_MSG
        GRPC_LIBRARIES
        )

mark_as_advanced(
        GRPC_LIBRARIES
)

message(STATUS "Found grpc (library: ${GRPC_LIBRARIES})")
