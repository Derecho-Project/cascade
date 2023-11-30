using System;
using System.Runtime.InteropServices;

namespace GatewayLib
{

    /// <summary>
    /// The most basic UDL, implemented in C#: a console printer which
    /// has no side effects and simply prints the metadata of an ocdpo handler call.
    /// </summary>
    public class ConsolePrinterUDL
    {
        public unsafe static void OcdpoHandler(
            UInt32 sender,
            IntPtr objectPoolPathname,
            IntPtr keyString,
            IntPtr objectKey,
            IntPtr objectBytes,
            UInt64 objectBytesSize,
            UInt32 workerId,
            IntPtr emitFunc,
            IntPtr emitInvokePtr)
        {   
            string key = Marshal.PtrToStringAnsi(keyString);
            System.Console.WriteLine("[C# .NET console printer ocdpo]: I: " + workerId 
                + " received an object from sender: " + sender + ", with key = " + key + ".");
        }
    }
}