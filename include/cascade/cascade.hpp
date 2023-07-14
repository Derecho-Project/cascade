#pragma once

/**
 * @file        cascade.hpp
 * @brief       This file includes the essential core cascade header files.
 *
 * @mainpage    The Cascade API Documentation
 *
 * @section intro_sec Introduction
 *
 * Cascade is a cloud application framework powered by optimized RDMA data paths. It is a BSD-3-Clause opensource
 * project. [Here](https://github.com/derecho-project/cascade/) is the official website. This documentation is the
 * Cascade developer's reference manual.
 *
 * @section api_sec The Cascade API.
 * TODO: Add pointers to Cascade concepts, installation manual, architecture documentation. Then describe the API.
 *
 * @subsection client_api_sec The Cascade Client API (C/C++, Python, C#)
 * TODO:
 * @subsection lambda_api_sec The Cascade UDL a.k.a. Lambda API (C/C++, Python, C#)
 * TODO:
 *
 * @section example_sec Example Applications
 * @subsection print_console_exp_sec Simple Lambda Examples (Console Printer, Wordcount, etc...)
 * TODO:
 * @subsection cms_exp_sec  A Pub/Sub System
 * TODO:
 * @subsection ynet_exp_sec Collision Prediction
 * TODO:
 *
 * @section Known Issues
 * TODO:
 */

#include <cascade/config.h>
#include "cascade_interface.hpp"
#include "volatile_store.hpp"
#include "persistent_store.hpp"
#include "trigger_store.hpp"
