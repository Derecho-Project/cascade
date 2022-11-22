#pragma once

#include <cascade/config.h>

#ifdef ENABLE_EVALUATION
#define TLT_PIPELINE(x)     (10000+x)
#define TLT_READY_TO_SEND   TLT_PIPELINE(1000)
#define TLT_EC_SENT         TLT_PIPELINE(2000)
#endif
