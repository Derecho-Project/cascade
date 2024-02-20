// Code based on https://docs.microsoft.com/en-us/dotnet/core/tutorials/netcore-hosting

#include <iostream>
#include <string.h>
#include "coreclrhost.hpp"
#include "gateway_to_managed.hpp"
#include <stdio.h>
#include <dlfcn.h>

#define MANAGED_ASSEMBLY "GatewayLib.dll"

#define CORECLR_DIR "/usr/share/dotnet/shared/Microsoft.NETCore.App/6.0.11"

GatewayToManaged::GatewayToManaged() {}

bool GatewayToManaged::Init(const std::string& master_absolute_dll_path) {
	// Construct the CoreCLR path to coreclr.dll/libcoreclr.so
	std::string coreClrPath(CORECLR_DIR);
	coreClrPath.append(FS_SEPERATOR);
	coreClrPath.append(CORECLR_FILE_NAME);

    // Load CoreCLR (coreclr.dll/libcoreclr.so)
	_core_clr = dlopen(coreClrPath.c_str(), RTLD_NOW | RTLD_LOCAL);
	if (_core_clr == nullptr) {
		std::cout << "ERROR: Failed to load CoreCLR from " << coreClrPath.c_str() << std::endl;
		return false;
	} else {
		std::cout << "Loaded CoreCLR from " << coreClrPath.c_str() << std::endl;
	}

	coreclr_initialize_ptr initialize_core_clr = (coreclr_initialize_ptr) dlsym(_core_clr, "coreclr_initialize");

	if (initialize_core_clr == nullptr) {
		std::cout << "coreclr_initialize not found" << std::endl;
		return false;
	}

	// Construct the trusted platform assemblies (TPA) list
	// This is the list of assemblies that .NET Core can load as trusted system assemblies.
	std::string tpa_list;
	build_tpa_list(CORECLR_DIR, ".dll", tpa_list);
	build_tpa_list(master_absolute_dll_path.c_str(), ".dll", tpa_list);

	// Define CoreCLR properties
	// Other properties related to assembly loading are common here, 
	// but for this simple sample, TRUSTED_PLATFORM_ASSEMBLIES is all that is needed. 
	// Check hosting documentation for other common properties.
	const char* propertyKeys[] = { "TRUSTED_PLATFORM_ASSEMBLIES" };     // trusted assemblies
	const char* propertyValues[] = { tpa_list.c_str() };

	// Start the CoreCLR runtime

	// This function both starts the .NET Core runtime and creates
	// the default (and only) AppDomain
	int hr = initialize_core_clr(
		CORECLR_DIR,                            // App base path
		"SampleHost",                           // AppDomain friendly name
		sizeof(propertyKeys) / sizeof(char*),   // Property count
		propertyKeys,                           // Property names
		propertyValues,                         // Property values
		&_host_handle,                           // Host handle
		&_domain_id);                            // AppDomain ID

	// Create managed delegate
	if (hr >= 0) {
		std::cout << "[gateway to managed]: CoreCLR started." << std::endl;
		_managed_direct_method = create_managed_delegate();
		return true;
	} else {
		std::cout << "coreclr_initialize failed - status: " << hr << std::endl;
		return false;
	}
}

// POSIX directory search for .dll files
void GatewayToManaged::build_tpa_list(const char* directory, const char* extension, std::string& tpa_list) {
	DIR* dir = opendir(directory);
	struct dirent* entry;
	int extLength = strlen(extension);

	while (dir != nullptr && (entry = readdir(dir)) != nullptr) {
		// This simple sample doesn't check for symlinks
		std::string filename(entry->d_name);

		// Check if the file has the right extension
		int extPos = filename.length() - extLength;
		if (extPos <= 0 || filename.compare(extPos, extLength, extension) != 0) {
			continue;
		}
		// Append the assembly to the list
		tpa_list.append(directory);
		tpa_list.append(FS_SEPERATOR);
		tpa_list.append(filename);
		tpa_list.append(PATH_DELIMITER);
	}
}

managed_direct_method_ptr GatewayToManaged::create_managed_delegate() {
	coreclr_create_delegate_ptr createManagedDelegate = 
		(coreclr_create_delegate_ptr) dlsym(_core_clr, "coreclr_create_delegate");

	if (createManagedDelegate == nullptr) {
		std::cout << "coreclr_create_delegate not found" << std::endl;
		return nullptr;
	}

	managed_direct_method_ptr managed_direct_method;
	int hr = createManagedDelegate(_host_handle,	_domain_id,
						"GatewayLib",
						"GatewayLib.Gateway",
						"ManagedDirectMethod",
						(void**)&managed_direct_method);

	if (hr >= 0) {
		std::cout << "[gateway to managed]: managed delegate created." << std::endl;
		return managed_direct_method;
	} else {
		std::cout << "[gateway to managed]: Error: coreclr_create_delegate failed - status: " << hr << std::endl;
		return nullptr;
	}
}

void GatewayToManaged::Invoke(const char* dllPath, const char* className, 
	const OcdpoArgs& ocdpoArgs, emit_callback_ptr emitInvokePtr) {
	return _managed_direct_method(dllPath, className, ocdpoArgs, emitInvokePtr);
}

bool GatewayToManaged::Close() {
	coreclr_shutdown_ptr shutdownCoreClr = (coreclr_shutdown_ptr)dlsym(_core_clr, "coreclr_shutdown");

	if (shutdownCoreClr == nullptr) {
		std::cout << "coreclr_shutdown not found" << std::endl;
		return false;
	}

	int hr = shutdownCoreClr(_host_handle, _domain_id);

	if (hr >= 0) {
		std::cout << "CoreCLR successfully shutdown" << std::endl;
	} else {
		std::cout << "coreclr_shutdown failed - status: " << hr << std::endl;
	}
	
	if (dlclose(_core_clr)) {
		std::cout << "Failed to free libcoreclr.so" << std::endl;
	}
	return true;
}

GatewayToManaged::~GatewayToManaged() {}
