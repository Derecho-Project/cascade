// Code based on https://docs.microsoft.com/en-us/dotnet/core/tutorials/netcore-hosting

#include <iostream>
#include <string.h>

using namespace std;

#define MANAGED_ASSEMBLY "GatewayLib.dll"

#if WINDOWS
#define CORECLR_DIR "C:\\Program Files\\dotnet\\shared\\Microsoft.NETCore.App\\6.0.8"
#elif LINUX
// https://github.com/dotnet/core-setup/issues/3078
#define CORECLR_DIR "/usr/share/dotnet/shared/Microsoft.NETCore.App/2.1.30"
#endif

#include "GatewayToManaged.h"

GatewayToManaged::GatewayToManaged()
{
}

bool GatewayToManaged::Init(const char* path)
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
	managedLibraryPath.append(MANAGED_ASSEMBLY);

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
	string tpaList;
	BuildTpaList(CORECLR_DIR, ".dll", tpaList);
	BuildTpaList(runtimePath, ".dll", tpaList);

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
		_managedDirectMethod = CreateManagedDelegate();
		return true;
	}
	else
	{
		cout << "coreclr_initialize failed - status: " << hr << endl;
		return false;
	}
}

#if WINDOWS
// Win32 directory search for .dll files
void GatewayToManaged::BuildTpaList(const char* directory, const char* extension, string& tpaList)
{
	// This will add all files with a .dll extension to the TPA list. 
	// This will include unmanaged assemblies (coreclr.dll, for example) that don't
	// belong on the TPA list. In a real host, only managed assemblies that the host
	// expects to load should be included. Having extra unmanaged assemblies doesn't
	// cause anything to fail, though, so this function just enumerates all dll's in
	// order to keep this sample concise.
	string searchPath(directory);
	searchPath.append(FS_SEPERATOR);
	searchPath.append("*");
	searchPath.append(extension);

	WIN32_FIND_DATAA findData;
	HANDLE fileHandle = FindFirstFileA(searchPath.c_str(), &findData);

	if (fileHandle != INVALID_HANDLE_VALUE)
	{
		do
		{
			// Append the assembly to the list
			tpaList.append(directory);
			tpaList.append(FS_SEPERATOR);
			tpaList.append(findData.cFileName);
			tpaList.append(PATH_DELIMITER);

			// Note that the CLR does not guarantee which assembly will be loaded if an assembly
			// is in the TPA list multiple times (perhaps from different paths or perhaps with different NI/NI.dll
			// extensions. Therefore, a real host should probably add items to the list in priority order and only
			// add a file if it's not already present on the list.
			//
			// For this simple sample, though, and because we're only loading TPA assemblies from a single path,
			// and have no native images, we can ignore that complication.
		} while (FindNextFileA(fileHandle, &findData));
		FindClose(fileHandle);
	}
}
#elif LINUX
// POSIX directory search for .dll files
void GatewayToManaged::BuildTpaList(const char* directory, const char* extension, string& tpaList)
{
	DIR* dir = opendir(directory);
	struct dirent* entry;
	int extLength = strlen(extension);

	while ((entry = readdir(dir)) != NULL)
	{
		// This simple sample doesn't check for symlinks
		string filename(entry->d_name);

		// Check if the file has the right extension
		int extPos = filename.length() - extLength;
		if (extPos <= 0 || filename.compare(extPos, extLength, extension) != 0)
			continue;

		// Append the assembly to the list
		tpaList.append(directory);
		tpaList.append(FS_SEPERATOR);
		tpaList.append(filename);
		tpaList.append(PATH_DELIMITER);

		// Note that the CLR does not guarantee which assembly will be loaded if an assembly
		// is in the TPA list multiple times (perhaps from different paths or perhaps with different NI/NI.dll
		// extensions. Therefore, a real host should probably add items to the list in priority order and only
		// add a file if it's not already present on the list.
		//
		// For this simple sample, though, and because we're only loading TPA assemblies from a single path,
		// and have no native images, we can ignore that complication.
	}
}
#endif

managed_direct_method_ptr GatewayToManaged::CreateManagedDelegate()
{
#if WINDOWS
	coreclr_create_delegate_ptr createManagedDelegate = (coreclr_create_delegate_ptr)GetProcAddress(_coreClr, "coreclr_create_delegate");
#elif LINUX    
	coreclr_create_delegate_ptr createManagedDelegate = (coreclr_create_delegate_ptr)dlsym(_coreClr, "coreclr_create_delegate");
#endif

	if (createManagedDelegate == NULL)
	{
		cout << "coreclr_create_delegate not found" << endl;
		return NULL;
	}

	managed_direct_method_ptr managedDirectMethod;

	int hr = createManagedDelegate(_hostHandle,	_domainId,
						"GatewayLib",
						"GatewayLib.Gateway",
						"ManagedDirectMethod",
						(void**)&managedDirectMethod);

	if (hr >= 0)
	{
		cout << "Managed delegate created" << endl;
		return managedDirectMethod;
	}
	else
	{
		cout << "coreclr_create_delegate failed - status: " << hr << endl;
		return NULL;
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
