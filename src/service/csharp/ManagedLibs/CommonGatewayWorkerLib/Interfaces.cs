using System;
using System.Collections.Generic;

namespace CommonGatewayWorkerLib
{
    public interface IWorker
    {
        Dictionary<string, Func<string, string, UnmanagedCallbackDelegate, string>> Functions { get; }
    }

    public delegate bool UnmanagedCallbackDelegate(string funcName, string jsonArgs);
}
