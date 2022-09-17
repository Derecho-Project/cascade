#include <iostream>
#include <string.h>
#include <stdbool.h>
#include "jsmn.h"

// Callback function passed to managed code to facilitate calling back into native code with status
bool UnmanagedCallback(const char* actionName, const char* jsonArgs);
