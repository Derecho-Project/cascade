using System;
using System.Runtime.InteropServices;

namespace GatewayLib
{
    /// <summary>
    /// UDL responsible for reporting word counts from their reduced values.
    /// </summary>
    public class WordCountReportUDL
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
            Int32 count = (Int32) Marshal.ReadByte(objectBytes);
            Console.WriteLine("[word count report]: " + key + ": " + count);
        }
    }
}