using System.Text.Json;

namespace Derecho.Cascade 
{

    using NodeId = Int64;

    public interface DefaultOffCriticalDataPathObserver 
    {
        string GetUuid();

        string GetDescription();

        void Initialize(ICascadeContext context);

        DefaultOffCriticalDataPathObserver GetObserver(ICascadeContext context,
            JsonObject udlConfig);

        void Release();

        // TODO: use correct type for blob instead of Object
        void OcdpoHandler(NodeId sender, string objectPoolPathname, string keyString, 
            ObjectWithStringKey obj, Action<string, Object> emit, 
            DefaultCascadeContextType typedCtxt, UInt32 workerId);
    }
} // namespace Derecho.Cascade