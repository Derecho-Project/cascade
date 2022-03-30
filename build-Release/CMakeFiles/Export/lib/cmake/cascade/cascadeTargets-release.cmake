#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "derecho::cascade" for configuration "Release"
set_property(TARGET derecho::cascade APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(derecho::cascade PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libcascade.so.1.0rc0"
  IMPORTED_SONAME_RELEASE "libcascade.so.1.0rc0"
  )

list(APPEND _IMPORT_CHECK_TARGETS derecho::cascade )
list(APPEND _IMPORT_CHECK_FILES_FOR_derecho::cascade "${_IMPORT_PREFIX}/lib/libcascade.so.1.0rc0" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
