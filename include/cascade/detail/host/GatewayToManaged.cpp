// Code based on https://docs.microsoft.com/en-us/dotnet/core/tutorials/netcore-hosting

#include <iostream>
#include <string.h>

using namespace std;

#if WINDOWS
#define CORECLR_DIR "C:\\Program Files\\dotnet\\shared\\Microsoft.NETCore.App\\6.0.8"
#elif LINUX
// https://github.com/dotnet/core-setup/issues/3078
// TODO: accept other versions of dotnet without manual hardcoding.
#define CORECLR_DIR "/usr/share/dotnet/shared/Microsoft.NETCore.App/2.1.30"
#endif

#include "GatewayToManaged.h"

GatewayToManaged::GatewayToManaged()
{
}

bool GatewayToManaged::Init(const char* path, const char* assembly_name)
{
	// Get the current executable's directory
	char runtimePath[MAX_PATH];
#if WINDOWS
	GetFullPathNameA(path, MAX_PATH, runtimePath, NULL);
#elif LINUX
	realpath(path, runtimePath);
#endif
	char *last_slash = strrchr(runtimePath, FS_SEPERATOR[0]);
	if (last_slash != NULL)
		*last_slash = 0;

	// Construct the CoreCLR path to coreclr.dll/libcoreclr.so
	string coreClrPath(CORECLR_DIR);
	coreClrPath.append(FS_SEPERATOR);
	coreClrPath.append(CORECLR_FILE_NAME);

	// Construct the managed library path
	string managedLibraryPath(runtimePath);
	managedLibraryPath.append(FS_SEPERATOR);
	managedLibraryPath.append(assembly_name);

    // Load CoreCLR (coreclr.dll/libcoreclr.so)
#if WINDOWS
	_coreClr = LoadLibraryExA(coreClrPath.c_str(), NULL, 0);
#elif LINUX 
	_coreClr = dlopen(coreClrPath.c_str(), RTLD_NOW | RTLD_LOCAL);
#endif 

	if (_coreClr == NULL)
	{
		cout << "ERROR: Failed to load CoreCLR from " << coreClrPath.c_str() << endl;
		return false;
	}
	else
		cout << "Loaded CoreCLR from " << coreClrPath.c_str() << endl;

#if WINDOWS
	coreclr_initialize_ptr initializeCoreClr = (coreclr_initialize_ptr)GetProcAddress(_coreClr, "coreclr_initialize");
#elif LINUX    
	coreclr_initialize_ptr initializeCoreClr = (coreclr_initialize_ptr)dlsym(_coreClr, "coreclr_initialize");
#endif

	if (initializeCoreClr == NULL)
	{
		cout << "coreclr_initialize not found" << endl;
		return false;
	}

	// Construct the trusted platform assemblies (TPA) list
	// This is the list of assemblies that .NET Core can load as trusted system assemblies.
	// For this host (as with most), assemblies next to CoreCLR will be included in the TPA list
	const string tpaList = path;

	// Define CoreCLR properties
	// Other properties related to assembly loading are common here, 
	// but for this simple sample, TRUSTED_PLATFORM_ASSEMBLIES is all that is needed. 
	// Check hosting documentation for other common properties.
	const char* propertyKeys[] = { "TRUSTED_PLATFORM_ASSEMBLIES" };     // trusted assemblies
	const char* propertyValues[] = { tpaList.c_str() };

	// Start the CoreCLR runtime

	// This function both starts the .NET Core runtime and creates
	// the default (and only) AppDomain
	int hr = initializeCoreClr(
		CORECLR_DIR,                            // App base path
		"SampleHost",                           // AppDomain friendly name
		sizeof(propertyKeys) / sizeof(char*),   // Property count
		propertyKeys,                           // Property names
		propertyValues,                         // Property values
		&_hostHandle,                           // Host handle
		&_domainId);                            // AppDomain ID

	// Create managed delegate
	if (hr >= 0)
	{
		cout << "CoreCLR started" << endl;
		_managedDirectMethod = CreateManagedDelegate(assembly_name);
		return true;
	}
	else
	{
		cout << "coreclr_initialize failed - status: " << hr << endl;
		return false;
	}
}

managed_direct_method_ptr GatewayToManaged::CreateManagedDelegate(const string& assembly_name)
{
#if WINDOWS
	coreclr_create_delegate_ptr createManagedDelegate = (coreclr_create_delegate_ptr)GetProcAddress(_coreClr, "coreclr_create_delegate");
#elif LINUX    
	coreclr_create_delegate_ptr createManagedDelegate = (coreclr_create_delegate_ptr)dlsym(_coreClr, "coreclr_create_delegate");
#endif

	if (createManagedDelegate == nullptr)
	{
		cout << "coreclr_create_delegate not found" << endl;
		return nullptr;
	}

	managed_direct_method_ptr managedDirectMethod;

	int hr = createManagedDelegate(
						_hostHandle,	
						_domainId,
						// Remove the .dll extension from the assembly file name.
						assembly_name.substr(0, assembly_name.size() - 4).c_str(),
						"Derecho.Cascade.wwDotnetBridgeFactory",
						"CreateDotNetBridgeByRef",
						(void**)&managedDirectMethod);

	if (hr >= 0)
	{
		cout << "Managed delegate created" << endl;
		return managedDirectMethod;
	}
}

char* GatewayToManaged::Invoke(const char* funcName, const char* jsonArgs, unmanaged_callback_ptr unmanagedCallback) 
{
	return _managedDirectMethod(funcName, jsonArgs, unmanagedCallback);
}

bool GatewayToManaged::Close()
{
#if WINDOWS
	coreclr_shutdown_ptr shutdownCoreClr = (coreclr_shutdown_ptr)GetProcAddress(_coreClr, "coreclr_shutdown");
#elif LINUX    
	coreclr_shutdown_ptr shutdownCoreClr = (coreclr_shutdown_ptr)dlsym(_coreClr, "coreclr_shutdown");
#endif

	if (shutdownCoreClr == NULL)
	{
		cout << "coreclr_shutdown not found" << endl;
		return false;
	}

	int hr = shutdownCoreClr(_hostHandle, _domainId);

	if (hr >= 0)
		cout << "CoreCLR successfully shutdown" << endl;
	else
		cout << "coreclr_shutdown failed - status: " << hr << endl;

	// Unload CoreCLR
#if WINDOWS
	if (!FreeLibrary(_coreClr))
		cout << "Failed to free coreclr.dll" << endl;
#elif LINUX
	if (dlclose(_coreClr))
		cout << "Failed to free libcoreclr.so" << endl;
#endif
	return true;
}

GatewayToManaged::~GatewayToManaged()
{
}
