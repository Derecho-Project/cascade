using System;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using System.IO;
using System.Threading;
using System.Linq;

namespace GatewayLib
{
    /// <summary>
    /// UDL responsible for reducing intermediate word count pairs to final results.
    /// </summary>
    public class WordCountReducerUDL
    {
        public static Dictionary<string, Int32> accumulatedCounters = 
            new Dictionary<string, Int32>();
        
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
            if (accumulatedCounters.ContainsKey(key))
            {
                accumulatedCounters[key] += count;
            }
            else 
            {
                accumulatedCounters[key] = count;
            }
            Console.WriteLine("[word count reducer]: Word count of key " + key + ": " 
                + accumulatedCounters[key]);


            IntPtr emitBytesPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int32)));
            try
            {
                Int32 val = accumulatedCounters[key];
                Marshal.StructureToPtr(val, emitBytesPtr, false);  
                InvokeEmit emit = 
                        (InvokeEmit) Marshal.GetDelegateForFunctionPointer(emitInvokePtr, typeof(InvokeEmit));
                emit(emitFunc, keyString, emitBytesPtr, 1);
            }
            catch (Exception)
            {
                Console.WriteLine("[word_count_reducer]: Exception caught when emitting data.");   
            }
            finally
            {
                // Avoid memory leaks by freeing all memory allocated here.
                // We don't free keyString, since it is owned by the native code.
                Marshal.FreeHGlobal(emitBytesPtr);
            }
        }
    }
} // namespace GatewayLib
