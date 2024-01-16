using System;
using System.Collections.Generic;
using System.Reflection;

namespace GatewayLib
{
    public class ReflectionLogic
    {
        private Dictionary<string, MethodInfo> dllPathToMethodInfo = 
            new Dictionary<string, MethodInfo>();

        public unsafe void Process(string dllAbsolutePath, string className,
            OcdpoArgsInternal* ocdpoArgsInternal, 
            IntPtr emitInvokePtr) 
        {
            Console.WriteLine("[csharp ocdpo (managed)]: DLL ABSOLUTE PATH: " + dllAbsolutePath);
            Console.WriteLine("[csharp ocdpo (managed)]: CLASS NAME: " + className);

            if (String.IsNullOrEmpty(dllAbsolutePath))
            {
                Console.WriteLine("[csharp ocdpo (managed)]: CoreCLR master DLL initialized.");
                return;
            }
            MethodInfo methodInfo;
            if (dllPathToMethodInfo.ContainsKey(dllAbsolutePath)) 
            {
                methodInfo = dllPathToMethodInfo[dllAbsolutePath];
            } 
            else 
            {
                Assembly assembly = Assembly.LoadFile(dllAbsolutePath);
                Type type = assembly.GetType(className);
                methodInfo = type.GetMethod("OcdpoHandler");
                dllPathToMethodInfo[dllAbsolutePath] = methodInfo;
            }

            methodInfo.Invoke(null, new object[]{
                    ocdpoArgsInternal->sender,
                    ocdpoArgsInternal->object_pool_pathname,
                    ocdpoArgsInternal->key_string,
                    ocdpoArgsInternal->object_key,
                    ocdpoArgsInternal->object_bytes,
                    ocdpoArgsInternal->object_bytes_size,
                    ocdpoArgsInternal->worker_id,
                    ocdpoArgsInternal->emit_func,
                    emitInvokePtr});
        }
    }
} // namespace GatewayLib
