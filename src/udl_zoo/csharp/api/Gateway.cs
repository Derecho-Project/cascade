using System;
using System.Reflection;
using System.Runtime.InteropServices;

namespace GatewayLib
{
    // This static class provides method to be called from unmanaged code
    public static class Gateway
    {
        public static ReflectionLogic _observer;
        static Gateway()
        {
            _observer = new ReflectionLogic();
        }
        // This method is called from unmanaged code
        public unsafe static void ManagedDirectMethod(
            [MarshalAs(UnmanagedType.LPStr)] string dllAbsolutePath,
            [MarshalAs(UnmanagedType.LPStr)] string className,
            OcdpoArgsInternal* ocdpoArgs,
            IntPtr emitInvokePtr)
        {
           _observer.Process(dllAbsolutePath, className, ocdpoArgs, emitInvokePtr);
        }
    }
} // namespace GatewayLib
