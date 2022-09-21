#pragma once

#include "coreclrhost.h" // https://github.com/dotnet/coreclr/blob/master/src/coreclr/hosts/inc/coreclrhost.h

using namespace std;

// Function pointer types for the managed call and unmanaged callback
typedef bool (*unmanaged_callback_ptr)(const char* actionName, const char* jsonArgs);
typedef char* (*managed_direct_method_ptr)(const char* actionName, 
	const char* jsonArgs, unmanaged_callback_ptr unmanagedCallback);

class GatewayToManaged
{
public:
	GatewayToManaged();
	~GatewayToManaged();

	bool Init(const char* path, const char* assembly_name);
	char* Invoke(const char* funcName, const char* jsonArgs, unmanaged_callback_ptr unmanagedCallback);
	bool Close();

private:
	void* _hostHandle;
	unsigned int _domainId;
	managed_direct_method_ptr _managedDirectMethod;
	managed_direct_method_ptr CreateManagedDelegate(const string& assembly_name);

#if WINDOWS
	HMODULE _coreClr;
#elif LINUX 
	void* _coreClr;
#endif
};

