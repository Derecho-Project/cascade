using System.Collections.Generic;  

namespace Derecho.Cascade 
{
    public interface DefaultOffCriticalDataPathObserver 
    {
        string GetUuid();

        string GetDescription();

        void Initialize(ICascadeContext context);

        DefaultOffCriticalDataPathObserver GetObserver(ICascadeContext context,
            Dictionary<string, System.Object> udlConfig);

        void Release();

        // TODO: use correct type for blob instead of Object
        void OcdpoHandler(System.Int64 sender, string objectPoolPathname, string keyString, 
            ObjectWithStringKey obj, System.Action<string, System.Object> emit, 
            DefaultCascadeContextType typedCtxt, System.UInt32 workerId);
    }
} // namespace Derecho.Cascade