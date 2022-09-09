# This module defines
# Readline_FOUND, if false, GNU Readline (or its dependency ncurses) is not installed
# Readline_LIBRARIES, the name of the library to link against
# Readline_INCLUDE_DIRS, where to find readline/readline.h

# Search for the path containing library's headers
find_path(Readline_ROOT_DIR
    NAMES include/readline/readline.h
)

# Search for include directory
find_path(Readline_INCLUDE_DIR
    NAMES readline/readline.h
    HINTS ${Readline_ROOT_DIR}/include
)

# Search for library
find_library(Readline_LIBRARY
    NAMES readline
    HINTS ${Readline_ROOT_DIR}/lib
)

# readline depends on libncurses, or similar
find_library(Ncurses_LIBRARY
    NAMES ncurses ncursesw curses termcap
    HINTS ${READLINE_ROOT_DIR}/lib
)

# Conditionally set READLINE_FOUND value
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Readline DEFAULT_MSG
    Readline_LIBRARY Readline_INCLUDE_DIR Ncurses_LIBRARY)

# Hide these variables in cmake GUIs
mark_as_advanced(
    Readline_ROOT_DIR
    Readline_INCLUDE_DIR
    Readline_LIBRARY
)

if(Readline_FOUND)
    set(Readline_INCLUDE_DIRS ${Readline_INCLUDE_DIR})
    set(Readline_LIBRARIES ${Readline_LIBRARY})
endif()
