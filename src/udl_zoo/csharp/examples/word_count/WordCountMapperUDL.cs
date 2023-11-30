using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.IO;
using System.Linq;

namespace GatewayLib
{
    /// <summary>
    /// UDL responsible for computation of the intermediate values for word counting.
    /// file_name:string -> key:count pairs.
    /// </summary>
    public class WordCountMapperUDL
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
            string text = Marshal.PtrToStringAnsi(objectBytes, (Int32) objectBytesSize);
            Console.WriteLine("[word count mapper]: Text: " + text);
            Dictionary<string, Int32> wordCount = new Dictionary<string, Int32>();

            foreach (string word in text.Split(' '))
            {
                if (wordCount.ContainsKey(word))
                {
                    wordCount[word]++;
                }
                else
                {
                    wordCount[word] = 1;
                }
            }
            foreach (string word in wordCount.Keys)
            {
                IntPtr emitKeyPtr = Marshal.StringToHGlobalAnsi(word);
                IntPtr emitBytesPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Int32)));            
                try
                {
                    Int32 val = 1;
                    Marshal.StructureToPtr(val, emitBytesPtr, false);
                    InvokeEmit emit = 
                    (InvokeEmit) Marshal.GetDelegateForFunctionPointer(emitInvokePtr, typeof(InvokeEmit));
                    emit(emitFunc, emitKeyPtr, emitBytesPtr, 1);
                }
                catch (Exception)
                {
                    Console.WriteLine("[word_count_mapper]: Exception caught when emitting data.");   
                }
                finally
                {
                    // Avoid memory leaks by freeing all memory allocated here.
                    Marshal.FreeHGlobal(emitKeyPtr);
                    Marshal.FreeHGlobal(emitBytesPtr);
                }
            }
        }
    }
}
 