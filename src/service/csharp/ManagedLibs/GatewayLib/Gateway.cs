using System;
using System.Reflection;
using System.Runtime.InteropServices;
using CommonGatewayWorkerLib;
using WorkerLib;
using LoggerLib;

namespace GatewayLib
{
    // This static class provides method to be called from unmanaged code
    public static class Gateway
    {
        private static Logger _logger = new Logger();
        private static IWorker _worker;

        static Gateway()
        {
            _worker = new Worker();
        }

        // This method is called from unmanaged code
        [return: MarshalAs(UnmanagedType.LPStr)]
        public static string ManagedDirectMethod(
            [MarshalAs(UnmanagedType.LPStr)] string funcName,
            [MarshalAs(UnmanagedType.LPStr)] string jsonArgs,
            UnmanagedCallbackDelegate dlgUnmanagedCallback)
        {
            _logger.Info($"ManagedDirectMethod(funcName: {funcName}, jsonArgs: {jsonArgs})");               

            string strRet = null;
            if (_worker.Functions.TryGetValue(funcName, out Func<string, string, UnmanagedCallbackDelegate, string> directCall))
            {
                try
                {
                    strRet = directCall(funcName, jsonArgs, dlgUnmanagedCallback);
                }
                catch (Exception e)
                {
                    strRet = $"ERROR in \"{funcName}\" invoke:{Environment.NewLine} {e}";
                    _logger.Error(strRet);
                }
            }

            return strRet;
        }
    }
}
