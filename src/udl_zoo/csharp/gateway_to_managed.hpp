#pragma once

#include "coreclrhost.hpp" // https://github.com/dotnet/coreclr/blob/master/src/coreclr/hosts/inc/coreclrhost.h
#include <cascade/object.hpp>
#include <cascade/service_types.hpp>
#include <cascade/user_defined_logic_interface.hpp>

// Struct representing all the ocdpo_handler args for a call.
// Made up of blittable types and pointers (IntPtr on the C# side).
// See this for a reference: https://learn.microsoft.com/en-us/dotnet/framework/interop/blittable-and-non-blittable-types
typedef struct {
	node_id_t sender;
	const char* const object_pool_pathname;
	const char* const key_string;
	const char* const object_key;
	const uint8_t* object_bytes;
	std::size_t object_bytes_size;
	uint32_t worker_id;
	const derecho::cascade::emit_func_t* emit_func;
} OcdpoArgs;

typedef void (*emit_callback_ptr)(const derecho::cascade::emit_func_t* emit_func, 
	const char* key, const uint8_t* bytes, const uint32_t size);
// Function pointer types for the managed call and unmanaged callback
// Representation of the ocdpo_handler (Process) in managed code.
typedef void (*managed_direct_method_ptr)(const char* dll_path,	const char* class_name, 
	const OcdpoArgs& ocdpo_args, emit_callback_ptr emit);

class GatewayToManaged
{
public:
	GatewayToManaged();
	~GatewayToManaged();

	bool Init(const std::string& master_absolute_dll_path);
	void Invoke(const char* dll_path, const char* class_ame, const OcdpoArgs& ocdpo_args, 
		emit_callback_ptr emit_invoke_ptr);
	bool Close();

private:
	bool _is_initialized;
	void* _host_handle;
	unsigned int _domain_id;
	managed_direct_method_ptr _managed_direct_method;

	void build_tpa_list(const char* directory, const char* extension, std::string& tpa_list);
	managed_direct_method_ptr create_managed_delegate();

	void* _core_clr;
};
