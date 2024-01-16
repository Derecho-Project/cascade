#include <cascade/config.h>
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/ndarraytypes.h>
#include <numpy/ndarrayobject.h>
#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <string>
#include <cstring>
#include <iomanip>
#include <chrono>
#include <cascade/object.hpp>
#include <dlfcn.h>
#include <queue>

#include "config.h"
#define PYTHONLIB   "libpython" Python3_VERSION_MAJOR "." Python3_VERSION_MINOR ".so"

/**
 * README
 *
 * This file implementes the python UDL wrapper for cascade. TODO: documentation
 *
 * Limitations
 * - Due to the non-multithreading nature and the numpy's incompatibility with multiple sub-interperters, this python
 *   UDL can only work in 'singlethreaded' mode. If you don't know what is that, please refer to cascade data_flow_graph
 *   configuration docuemnt in cascade/data_flow_graph.hpp and search for 'stateful'. 'singlethreaded' mode make sure
 *   that, at any time, there is at most ONE thread calling this python UDL. In the future, we plan to expand this to
 *   multi-process mode to parallelize python UDLs (of course, with some IPC overhead.)
 * - For low overhead, we currently use spin lock to avoid catastrophic crash.
 */

namespace derecho{
namespace cascade{

#define MY_UUID     "6cfe8f64-3a1d-11ed-8e7e-0242ac110006"
#define MY_DESC     "The python wrapper UDL."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

#define PYUDL_CONF_PYTHON_PATH  "python_path"
#define PYUDL_CONF_MODULE       "module"
#define PYUDL_CONF_ENTRY_CLASS  "entry_class"
#define PYUDL_MODULE_NAME       "derecho.cascade.udl"
#define PYUDL_BASE_TYPE         "UserDefinedLogic"
#define PYUDL_OCDPO_HANDLER     "ocdpo_handler"
// #define PYUDL_CONTEXT_MODULE    PYUDL_MODULE_NAME ".context"
#define PYUDL_CONTEXT_MODULE    "cascade_context"
#define PYUDL_PRELOAD_MODULES   "sys","os",PYUDL_MODULE_NAME /*"numpy", - numpy has its own C-API*/

class PythonOCDPO: public DefaultOffCriticalDataPathObserver {

    PyObject* python_observer;
    PyObject* python_ocdpo_handler_method;
public:
    /*
     * The constructor
     */
    PythonOCDPO(
            PyObject* _python_ocdpo,
            PyObject* _python_ocdpo_handler_func,
            DefaultCascadeContextType* typed_ctxt):
            python_observer(_python_ocdpo),
            python_ocdpo_handler_method(_python_ocdpo_handler_func) {}

    /*
     * The destructor
     */
    virtual ~PythonOCDPO() {
        if (python_observer) {
            Py_DECREF(python_observer);
        }
    }

private:    
    /* request type to python thread */
    struct python_request_t {
        enum {
            TERMINATE,
            EXECUTE_OCDPO,
            CREATE_OCDPO,
        } type;
        uint64_t sequence_num;
        union {
            std::monostate terminate;
            struct {
                PyObject*           handler_ptr;
                node_id_t     sender;
                const std::string*  object_pool_pathname_ptr;
                const std::string*  key_string_ptr;
                const ObjectWithStringKey*
                                    object_ptr;
                const emit_func_t*  emit_ptr;
                DefaultCascadeContextType*
                                    typed_ctxt;
                uint32_t      worker_id;
            } execute_ocdpo;
            struct {
                ICascadeContext*        ctxt;
                const nlohmann::json*   conf_ptr;
            } create_ocdpo;
        } request;
    };
    /* response type from python thread */
    struct python_response_t {
        uint64_t    sequence_num;
        bool        success;
        std::shared_ptr<PythonOCDPO>
                    ocdpo;
        python_response_t() = default;
    };

    virtual void ocdpo_handler (
            const node_id_t             sender,
            const std::string&          object_pool_pathname,
            const std::string&          key_string,
            const ObjectWithStringKey&  object,
            const emit_func_t&          emit,
            DefaultCascadeContextType*  typed_ctxt,
            uint32_t                    worker_id) override {

        dbg_default_trace("entering python_udl handler. with op={}, key={}", object_pool_pathname, key_string);

        struct python_request_t req;
        req.type = python_request_t::EXECUTE_OCDPO;
        req.request.execute_ocdpo.handler_ptr   = this->python_ocdpo_handler_method;
        req.request.execute_ocdpo.sender        = sender;
        req.request.execute_ocdpo.object_pool_pathname_ptr
                                            = &object_pool_pathname;
        req.request.execute_ocdpo.key_string_ptr
                                            = &key_string;
        req.request.execute_ocdpo.object_ptr    = &object;
        req.request.execute_ocdpo.emit_ptr      = &emit;
        req.request.execute_ocdpo.typed_ctxt    = typed_ctxt;
        req.request.execute_ocdpo.worker_id     = worker_id;

        auto response = post_request(req);

        if (!response.success) {
            dbg_default_error("{}:{} Failed to process the request sequence:{}", __FILE__,__LINE__,response.sequence_num);
        }

        dbg_default_trace("leaving python_udl handler.");
    }

/* ---- static members follow ---- */
private:
    /* singleton attributes */
    static wchar_t*             program_name;       // name of the program
    static std::atomic<bool>    python_initialized; // is python udl singleton initialized?
    static std::mutex           python_mutex;       // to protect python context
    static std::thread          python_thread;      // the python singleton thread

    static std::condition_variable
                                python_request_cv;  // the python singleton request condition variable
    static std::mutex           python_request_mutex;
    static std::queue<struct python_request_t>
                                python_request_queue;
    static uint64_t             python_request_sequence_number;

    static std::condition_variable
                                python_response_cv; // the python singleton response condition variable
    static std::mutex           python_response_mutex;
    static std::queue<struct python_response_t>
                                python_response_queue;

    static std::unordered_map<std::string,PyObject*>
                            imported_modules;       // a map holding pointers to all imported modules.
    static PyTypeObject*    udl_base_type;          // pointer to derecho.cascade.udl.UserDefinedLogic
    static PyMethodDef      context_methods[];      // context methods definition
    static PyModuleDef      context_module;         // context module definition
public:
    // global initializer
    static void initialize() {
        if (python_initialized) {
            return;
        }

        /* python lock */
        const std::lock_guard<std::mutex> lock(python_mutex);
        if (python_initialized) {
            return;
        }
        /* start the python thread */
        python_thread = std::thread([&](){
            /* 1. load python */
            auto handle = dlopen(PYTHONLIB, RTLD_NOW|RTLD_GLOBAL);
            if (handle==nullptr) {
                std::cerr << "failed to preload libpython with error:" << dlerror() << std::endl;
                return;
            }
            /* 2. detect if another interpreter is initialized or not. */
            if (Py_IsInitialized()) {
                dbg_default_error("Failed to initialize Python UDL because python interpreter is initialized already. {}:{}",
                    __FILE__,__LINE__);
                return;
            }
            /* 3. set up program name */
            program_name = Py_DecodeLocale("cascade_python_udl", NULL);
            if (program_name == nullptr) {
                dbg_default_warn("Failed to set name for the python interpreter. skipping... {}:{}",
                    __FILE__,__LINE__);
            } else {
                Py_SetProgramName(program_name);
            }
            /* 4. set up derecho.cascade.udl.context module */
            if (PyImport_AppendInittab(PYUDL_CONTEXT_MODULE, &PyInit_context)==-1){
                dbg_default_error("Failed to load extension {}, {}:{}",
                    PYUDL_CONTEXT_MODULE, __FILE__, __LINE__);
                PyErr_Print();
                return;
            }
            dbg_default_trace("Module: {} is registered.", PYUDL_CONTEXT_MODULE);
            /* 5. initialize with out signal handlers */
            Py_InitializeEx(0);
            /* --- Interpretor specific initializations follows --- */
            /* 6. import numpy, sys, os */
            if (_import_array() < 0 ) {
                dbg_default_error("Failed to import numpy. {}:{}", __FILE__,__LINE__);
                PyErr_Print();
                return;
            }
            /* 7. preload modules like */
            const std::vector<std::string> preloaded_modules = {
                PYUDL_PRELOAD_MODULES
            };
            for (const auto& m: preloaded_modules) {
                load_module(m.c_str());
            }
            /* 8. add cwd(".") to python path */
            if (append_python_path(".")!=0) {
                dbg_default_error("Failed to add current path to python path. {}:{}", __FILE__,__LINE__);
                return;
            }
            /* 9. load udl base type */
            if (imported_modules.find(PYUDL_MODULE_NAME)!=imported_modules.cend()) {
                udl_base_type = reinterpret_cast<PyTypeObject*>(
                    PyObject_GetAttrString(imported_modules.at(PYUDL_MODULE_NAME), PYUDL_BASE_TYPE));
                if (udl_base_type == nullptr || !PyType_Check(udl_base_type)) {
                    dbg_default_error("Failed to load udl base type: {}, {}:{}.", PYUDL_BASE_TYPE, __FILE__, __LINE__);
                    return;
                }
            } else {
                dbg_default_error("Giving up loading udl base type:{}, {}:{}", PYUDL_BASE_TYPE, __FILE__, __LINE__);
                return;
            }
            /* 10. handle the requests
             *
             * There are three types of requests
             * - thread termination
             * - get an observer
             * - process a list of requests.
             */
            bool alive = true;
            while (alive) {
                /* 10.1 pick the requests */
                std::unique_lock req_lock(python_request_mutex);
                python_request_cv.wait(req_lock,[&]{return !python_request_queue.empty();});
                std::queue<python_request_t> todo_list;
                python_request_queue.swap(todo_list);
                req_lock.unlock();
                /* 10.2 process it */
                while (!todo_list.empty()) {
                    // take first
                    struct python_request_t     req = todo_list.front();
                    struct python_response_t    res;
                    res.sequence_num = req.sequence_num;

                    dbg_default_trace("{}:{} [PYTHON] Processing request (type:{} sequence:{})",
                            __FILE__,__LINE__,req.type,req.sequence_num);

                    switch(req.type) {
                    case python_request_t::TERMINATE:
                        // we just drop the requests come after termination.
                        alive = false;
                        Py_FinalizeEx();
                        PyMem_RawFree(program_name);
                        res.success = true;
                        break;
                    case python_request_t::EXECUTE_OCDPO:
                        {
                            /* 10.2.2.1 set up the emit function */
                            dbg_default_trace("{}:{} register emit function.", __FILE__,__LINE__);
                            register_emit_func(req.request.execute_ocdpo.emit_ptr);
                            /* 10.2.2.2 set up the arguments */
                            dbg_default_trace("{}:{} setting up the arguments.", __FILE__,__LINE__);
                            PyObject* targs = PyTuple_New(0);
                            if (targs == nullptr) {
                                dbg_default_error("Failed to create a Python tuple object. {}:{}", __FILE__,__LINE__);
                                PyErr_Print();
                                res.success = false;
                                break;
                            }
                            PyObject* kwargs = PyDict_New();
                            if (kwargs == nullptr) {
                                dbg_default_error("Failed to create a Python dict object. {}:{}", __FILE__,__LINE__);
                                PyErr_Print();
                                res.success = false;
                                break;
                            }
                            PyObject* py_sender     = PyLong_FromLong(req.request.execute_ocdpo.sender);
                            PyObject* py_pathname   = PyUnicode_FromString(req.request.execute_ocdpo.object_pool_pathname_ptr->c_str());
                            PyObject* py_key        = PyUnicode_FromString(req.request.execute_ocdpo.key_string_ptr->c_str());
                            PyObject* py_version    = PyLong_FromLong(req.request.execute_ocdpo.object_ptr->version);
                            PyObject* py_timestamp_us
                                                    = PyLong_FromLong(req.request.execute_ocdpo.object_ptr->timestamp_us);
                            PyObject* py_previous_version
                                                    = PyLong_FromLong(req.request.execute_ocdpo.object_ptr->previous_version);
                            PyObject* py_previous_version_by_key
                                                    = PyLong_FromLong(req.request.execute_ocdpo.object_ptr->previous_version_by_key);
                            npy_intp  dims          = req.request.execute_ocdpo.object_ptr->blob.size;
                            PyObject* py_value_wrapper  = PyArray_NewFromDescr(
                                                          &PyArray_Type,
                                                          PyArray_DescrFromType(NPY_UINT8),
                                                          1,
                                                          &dims,
                                                          nullptr,
                                                          const_cast<void*>(static_cast<const void*>(req.request.execute_ocdpo.object_ptr->blob.bytes)),
                                                          0,
                                                          nullptr);
                            PyObject* py_worker_id  = PyLong_FromLong(req.request.execute_ocdpo.worker_id);
                            PyDict_SetItemString(kwargs,"sender",py_sender);
                            PyDict_SetItemString(kwargs,"pathname",py_pathname);
                            PyDict_SetItemString(kwargs,"key",py_key);
                            PyDict_SetItemString(kwargs,"version",py_version);
                            PyDict_SetItemString(kwargs,"timestamp_us",py_timestamp_us);
                            PyDict_SetItemString(kwargs,"previous_version",py_previous_version);
                            PyDict_SetItemString(kwargs,"previous_version_by_key",py_previous_version_by_key);
                            PyDict_SetItemString(kwargs,"blob",py_value_wrapper);
                            PyDict_SetItemString(kwargs,"worker_id",py_worker_id);
#ifdef  ENABLE_EVALUATION
                            PyObject* py_message_id = PyLong_FromLong(req.request.execute_ocdpo.object_ptr->message_id);
                            PyDict_SetItemString(kwargs,"message_id",py_message_id);
#endif
                            /* 10.2.2.3 call the handler*/
                            dbg_default_trace("{}:{} calling the handler.", __FILE__,__LINE__);
                            PyObject* ret = PyObject_Call(req.request.execute_ocdpo.handler_ptr,targs,kwargs);
                            if (ret == nullptr) {
                                dbg_default_error("Exception raised in user application. {}:{}", 
                                        __FILE__,__LINE__);
                                PyErr_Print();
                                res.success = false;
                                break;
                            } else {
                                Py_DECREF(ret);
                            }
                    
                            Py_DECREF(kwargs);
#ifdef  ENABLE_EVALUATION
                            Py_DECREF(py_message_id);
#endif
                            Py_DECREF(py_worker_id);
                            Py_DECREF(py_value_wrapper);
                            Py_DECREF(py_previous_version_by_key);
                            Py_DECREF(py_previous_version);
                            Py_DECREF(py_timestamp_us);
                            Py_DECREF(py_version);
                            Py_DECREF(py_key);
                            Py_DECREF(py_pathname);
                            Py_DECREF(py_sender);
                            Py_DECREF(targs);
                   
                            res.success = true;
                            dbg_default_trace("{}:{} User processing function returned.", __FILE__,__LINE__);
                        }
                        break;
                    case python_request_t::CREATE_OCDPO:
                        {
                            ICascadeContext* ctxt = req.request.create_ocdpo.ctxt;
                            const nlohmann::json* conf_ptr = req.request.create_ocdpo.conf_ptr;
                            /* 10.2.3.1 check/update python path */
                            dbg_default_trace("{}:{} check/update python path",__FILE__,__LINE__);
                            std::vector<std::string> python_path;
                            if (conf_ptr->contains(PYUDL_CONF_PYTHON_PATH)) {
                                python_path = (*conf_ptr)[PYUDL_CONF_PYTHON_PATH].get<std::vector<std::string>>();
                            }
                            for (const auto& pp: python_path) {
                                dbg_default_trace("Adding python path: {}", pp);
                                PythonOCDPO::append_python_path(pp.c_str());
                            }
                            /* 10.2.3.2 import the user's module */
                            std::string module_name;
                            if (conf_ptr->contains(PYUDL_CONF_MODULE)) {
                                module_name = (*conf_ptr)[PYUDL_CONF_MODULE].get<std::string>();
                                dbg_default_trace("{}:{} import the user's module: {}",__FILE__,__LINE__,module_name);
                                if(PythonOCDPO::load_module(module_name.c_str())) {
                                    dbg_default_error("Error: failed to load user module: {}, {}:{}",
                                            module_name, __FILE__, __LINE__);
                                    PyErr_Print();
                                    res.success = false;
                                    break;
                                }
                            } else {
                                dbg_default_error("Error: user module is not specified for python udl(uuid:{}). {}:{}",
                                        get_uuid(),__FILE__,__LINE__);
                                res.success = false;
                                break;
                            }
                            /* 10.2.3.3 create python handler object */
                            PyObject* python_ocdpo = nullptr;
                            if (conf_ptr->contains(PYUDL_CONF_ENTRY_CLASS)) {
                                std::string class_name = (*conf_ptr)[PYUDL_CONF_ENTRY_CLASS].get<std::string>();
                                dbg_default_trace("{}:{} create python handler object from class:{}",__FILE__,__LINE__,class_name);
                        
                                // we assure py_module will be valid because STEP 2 succeeded.
                                auto py_module = PythonOCDPO::get_module(module_name.c_str()); 
                                auto entry_class_type = PyObject_GetAttrString(py_module,class_name.c_str());
                                if (entry_class_type == nullptr || !PyType_Check(entry_class_type)) {
                                    dbg_default_error("Failed loading python udl entry class:{}.{}. {}:{}",
                                            module_name, class_name, __FILE__, __LINE__);
                                    PyErr_Print();
                                    res.success = false;
                                    break;
                                }
                        
                                // test if entry_class_type is a subclass of Type UserDefinedLogical.
                                if (!PythonOCDPO::is_valid_observer_type(reinterpret_cast<PyTypeObject*>(entry_class_type))) {
                                    dbg_default_error("Error: {} is not a subclass of derecho.cascade.udl.UserDefinedLogic. {}:{}",
                                            class_name, __FILE__, __LINE__);
                                    res.success = false;
                                    break;
                                }
                                
                                // create object
                                std::string conf_str = to_string(*conf_ptr);
                                auto conf_arg = Py_BuildValue("s",conf_str.c_str());
                                if (conf_arg == nullptr) {
                                    dbg_default_error("Error: Failed building value for python handler object. {}:{}",
                                            __FILE__, __LINE__);
                                    PyErr_Print();
                                    res.success = false;
                                    break;
                                }
                                auto pargs = PyTuple_New(1);
                                PyTuple_SetItem(pargs,0,conf_arg);
                                python_ocdpo = PyObject_Call(entry_class_type,pargs,nullptr);
                        
                                Py_DECREF(pargs);
                                // Py_DECREF(conf_arg); <-- don't do this: conf_arg has been 'stolen' by PyTuple_SetItem()
                                if (python_ocdpo == nullptr) {
                                    dbg_default_error("Error: Failed creating python handler object. {}:{}",
                                            __FILE__, __LINE__);
                                    PyErr_Print();
                                    res.success = false;
                                    break;
                                }
                            } else {
                                dbg_default_error("Error: user entry class is not specified for python udl(uuid:{}). {}:{}",
                                        get_uuid(),__FILE__,__LINE__);
                                res.success = false;
                                break;
                            }
                            /* 10.2.3.4 get python handler object's method */
                            dbg_default_trace("{}:{} get python handler object's method.", __FILE__,__LINE__);
                            PyObject* python_ocdpo_handler = PyObject_GetAttrString(python_ocdpo,PYUDL_OCDPO_HANDLER);
                            if (python_ocdpo_handler == nullptr) {
                                dbg_default_error("Error: Failed getting ocdpo handler from python user code. {}:{}",
                                        __FILE__, __LINE__);
                                res.success = false;
                                break;
                            }
                            if (!PyCallable_Check(python_ocdpo_handler)) {
                                dbg_default_error("Error: the ocdpo handler from python is not callable. {}:{}",
                                        __FILE__, __LINE__);
                                res.success = false;
                                break;
                            }
                            dbg_default_trace("{}:{} ocdpo handler method is created @{:p}", __FILE__,__LINE__,static_cast<void*>(python_ocdpo_handler));
                        
                            res.ocdpo = std::make_shared<PythonOCDPO>(python_ocdpo,python_ocdpo_handler,dynamic_cast<DefaultCascadeContextType*>(ctxt));
                            res.success = true;
                        }
                        break;
                    }
                    
                    dbg_default_trace("{}:{} [PYTHON] Finished processing request (type:{} sequence:{}), response.success={}",
                            __FILE__,__LINE__,req.type,req.sequence_num,res.success);

                    // notification
                    std::unique_lock res_lock(python_response_mutex);
                    python_response_queue.emplace(res);
                    res_lock.unlock();
                    python_response_cv.notify_all();
                    // pop it
                    todo_list.pop();

                    dbg_default_trace("{}:{} [PYTHON] Finished sending response (sequence:{} success:{}).",
                            __FILE__,__LINE__,res.sequence_num,res.success);
                }
            }
        });

        /* finish initialization. */
        python_initialized.store(true);
    }

    static void shutdown() {
        if (python_initialized) {
            dbg_default_trace("{}:{} calling shutdown().",__FILE__,__LINE__);
            std::lock_guard<std::mutex> lock(python_mutex);
            if (python_initialized) {
                reentrant_shutdown();
                python_initialized.store(false);
            }
            dbg_default_trace("{}:{} shutdown finished.",__FILE__,__LINE__);
        }
    }

    /*
     * post a request to the python request queue and wait for response.
     * @param request - the python request
     *
     * @return response
     */
    static python_response_t post_request(python_request_t& request) {
        python_response_t response;
        // insert a request to request queue.
        std::unique_lock<std::mutex> req_lock(python_request_mutex);
        request.sequence_num = python_request_sequence_number++;
        dbg_default_trace("{}:{} posting request (type:{} seq:{})",
                __FILE__,__LINE__,request.type,request.sequence_num);
        python_request_queue.emplace(request);
        req_lock.unlock();
        python_request_cv.notify_one();

        // wait for reply
        std::unique_lock<std::mutex> res_lock(python_response_mutex);
        python_response_cv.wait(res_lock,[&,seqno = request.sequence_num](){
                if(python_response_queue.empty()) {
                    return false;
                } else {
                    return (python_response_queue.front().sequence_num == seqno);
                }
            });
        response = python_response_queue.front();
        python_response_queue.pop();
        res_lock.unlock();
        dbg_default_trace("{}:{} request(type:{} seq:{}/{}) is responsed.",
                __FILE__,__LINE__, request.type, request.sequence_num, response.sequence_num);
        return response;
    }

    /*
     * Get the Observer
     * @param ctxt      cascade context
     * @param conf      the json configuration
     *
     * @return a shared pointer to the observer, or nullptr if it failed.
     */
    static std::shared_ptr<OffCriticalDataPathObserver> reentrant_get_observer(
        ICascadeContext* ctxt,const nlohmann::json& conf) {
        dbg_default_trace("{}:{} reentrant_get_observer() is called with conf: {}."
                __FILE__,__LINE__,conf.dump());
        python_request_t req;
        req.type = python_request_t::CREATE_OCDPO;
        req.request.create_ocdpo.ctxt = ctxt;
        req.request.create_ocdpo.conf_ptr = &conf;

        auto res = post_request(req);

        dbg_default_trace("{}:{} reentrant_get_observer() returns with success={}."
                __FILE__,__LINE__,res.success);
        if (!res.success) {
            return nullptr;
        }
        return res.ocdpo;
    }

    /*
     * Shutdown the thread
     */
    static void reentrant_shutdown() {
        python_request_t req;
        req.type = python_request_t::TERMINATE;
        auto res = post_request(req);

        if (res.success) {
            if (python_thread.joinable()) {
                python_thread.join();
            }
        } else {
            dbg_default_error("{}:{} TERMINATE request to python thread failed for some reason.", __FILE__,__LINE__);
        }
    }

private:
    /**
     * append python path
     */
    static int append_python_path(const char* path) {
        int ret = 1;

        if (imported_modules.find("sys") != imported_modules.cend()) {
            auto sys_path = PyObject_GetAttrString(imported_modules.at("sys"), "path");
            if (sys_path == nullptr) {
                dbg_default_error("Failed retrieving sys.path. Skipping update module search path:{}. {}:{}",
                        path,__FILE__,__LINE__);
                PyErr_Print();
            } else {
                auto none = PyObject_CallMethod(sys_path,"append","s",path);
                Py_DECREF(none);
                Py_DECREF(sys_path);
                ret = 0;
            }
        } else {
            dbg_default_warn("Python's 'sys' module is not loaded. Skipping update module search path:{}. {}:{}",
                    path,__FILE__,__LINE__);
        }

        return ret;
    }

    /*
     * load module
     */
    static int load_module(const char* module) {
        int ret = 0;

        // skip duplicated modules
        if (imported_modules.find(module) == imported_modules.cend()) {
            dbg_default_trace("Loading module:{}",module);
            // import
            auto py_module = PyImport_ImportModule(module);
            if (py_module == nullptr) {
                dbg_default_error("Failed loading python module:{} {}:{}", module, __FILE__,__LINE__);
                PyErr_Print();
                ret = 1;
            } else {
                imported_modules.emplace(module,py_module);
            }
        }

        return ret;
    }

    /*
     * get module
     */
    static PyObject* get_module(const char* module) {
        PyObject* ret = nullptr;
        // skip duplicated modules
        if (imported_modules.find(module) != imported_modules.cend()) {
            ret = imported_modules.at(module);
        }

        return ret;
    }

    /*
     * type validation
     */
    static bool is_valid_observer_type(PyTypeObject* observer_type) {
        bool ret = false;

        if (udl_base_type != nullptr &&
            (PyObject_IsSubclass(reinterpret_cast<PyObject*>(observer_type),
                                 reinterpret_cast<PyObject*>(udl_base_type)) == 1)) {
            ret = true;
        }

        return ret;
    }

    /* context service interface */
    static const emit_func_t* _emit_func;
    /*
     * Register the current emit function.
     *
     * @param emit_func_ptr -   The pointer to the current emit function
     */
    static void register_emit_func(const emit_func_t* emit_func_ptr) {
        _emit_func = emit_func_ptr;
    }
    /*
     * Emit a key/value pair
     *
     * The emit python signature:
     * def emit(key,value,**kwargs):
     *     '''
     *     emit an object to the next stage of the pipeline.
     *     key      -- (string) the key
     *     value    -- (numpy array of any shape) the value of the object
     *
     *     optional keys:
     *     version                  -- (int) the version of the emitted object
     *     timestamp_us             -- (int) the timestamp of the emitted object, default value is 0.
     *     previous_version         -- (int) the previous version of the emitted object, default value is INVALID_VERSION
     *     previous_version_by_key  -- (int) the previous version of the same key of the emitted object, default value is
     *                                 INVALID_VERSION
     *     message_id               -- (int) the message id of the key. default value is 0. This id is only valid when
     *                                 ENABLE_EVALUATION is defined.
     *
     *
     *     return value is None
     *     '''
     *     # C extension implementation
     *     pass
     *
     * @param self
     * @param args
     * @param kwargs
     *
     */
    static PyObject* emit(PyObject* self, PyObject* args, PyObject* kwargs) {
        /* STEP 1: Raise an assertion exception, in case _emit_func is not set. */
        if (_emit_func == nullptr) {
            Py_INCREF(PyExc_AssertionError);
            PyErr_SetString(PyExc_AssertionError,
                    "_emit_func is null. Is register_emit_func called with a valid function pointer?");
            return nullptr;
        }
        /* STEP 2: Extract the parameters and call _emit_func. */
        char* key = nullptr;
        PyObject* value = nullptr;
        persistent::version_t version = INVALID_VERSION;
        uint64_t              timestamp_us = 0;
        persistent::version_t previous_version = INVALID_VERSION;
        persistent::version_t previous_version_by_key = INVALID_VERSION;
#ifdef ENABLE_EVALUATION
        uint64_t              message_id = 0;
#endif
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
        static char* kwlist[] = {
                                 "key"
                                 ,"value"
                                 ,"version"
                                 ,"timestamp_us"
                                 ,"previous_version"
                                 ,"previous_version_by_key"
#ifdef ENABLE_EVALUATION
                                 ,"message_id"
#endif
                                 ,nullptr
                                };
#pragma GCC diagnostic pop
        if (!PyArg_ParseTupleAndKeywords(args,kwargs,
#ifdef ENABLE_EVALUATION
                                         "sO|LKLLK", kwlist,
                                         &key, &value, &version, &timestamp_us, &previous_version, &previous_version_by_key, &message_id
#else
                                         "sO|LKLL", kwlist,
                                         &key, &value, &version, &timestamp_us, &previous_version, &previous_version_by_key

#endif
                    )) {
            return nullptr;
        }

        if (!PyArray_Check(value)) {
            PyErr_SetString(PyExc_AssertionError,
                    "The second argument, value, is NOT a NumPy array!");
            return nullptr;
        }
        PyArrayObject *ndarray = reinterpret_cast<PyArrayObject*>(value);
        /* STEP 3: Call _emit_func. */
        uint8_t * data = reinterpret_cast<uint8_t*>(PyArray_DATA(ndarray));
        Blob blob_wrapper(data, static_cast<std::size_t>(PyArray_NBYTES(ndarray)), true);

        (*_emit_func)(std::string(key)
                     ,version
                     ,timestamp_us
                     ,previous_version
                     ,previous_version_by_key
#ifdef ENABLE_EVALUATION
                     ,message_id
#endif
                     ,blob_wrapper);
        
        Py_RETURN_NONE;
    }

    /*
     * creating the context module.
     */
    static PyObject* PyInit_context(void) {
        return PyModule_Create(&context_module);
    }
};

/* definition of static variables */
wchar_t*            PythonOCDPO::program_name  = nullptr;
std::unordered_map<std::string,PyObject*>
                    PythonOCDPO::imported_modules = {};
std::atomic<bool>   PythonOCDPO::python_initialized = false;
std::mutex          PythonOCDPO::python_mutex;
std::thread         PythonOCDPO::python_thread;
std::condition_variable
                    PythonOCDPO::python_request_cv;
std::mutex
                    PythonOCDPO::python_request_mutex;
std::queue<struct PythonOCDPO::python_request_t>
                    PythonOCDPO::python_request_queue;
uint64_t            PythonOCDPO::python_request_sequence_number = 0ull;
std::condition_variable
                    PythonOCDPO::python_response_cv;
std::mutex          PythonOCDPO::python_response_mutex;
std::queue<struct PythonOCDPO::python_response_t>
                    PythonOCDPO::python_response_queue;
PyTypeObject*       PythonOCDPO::udl_base_type = nullptr;
const emit_func_t*  PythonOCDPO::_emit_func = nullptr;

PyMethodDef PythonOCDPO::context_methods[]   = {
    {"emit", reinterpret_cast<PyCFunction>(&PythonOCDPO::emit), METH_VARARGS|METH_KEYWORDS,
     "emit object to next."},
    {nullptr,nullptr,0,nullptr}
};

PyModuleDef PythonOCDPO::context_module      = {
    PyModuleDef_HEAD_INIT,
    PYUDL_CONTEXT_MODULE, nullptr, -1, PythonOCDPO::context_methods,
    nullptr, nullptr, nullptr, nullptr
};

/* 
 * This will only be called once 
 */
void initialize(ICascadeContext* ctxt) {
    PythonOCDPO::initialize();
}

/* 
 * This will be called for each UDL(PythonOCDPO) instance.
 */
std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext* ctxt,const nlohmann::json& conf) {
    return PythonOCDPO::reentrant_get_observer(ctxt,conf);
}

void release(ICascadeContext* ctxt) {
    PythonOCDPO::shutdown();
}

} // cascade
} // derecho

