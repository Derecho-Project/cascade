using System.Text.Json;

namespace Derecho.Cascade {
    public interface DefaultOffCriticalDataPathObserver 
    {
        string GetUuid();

        string GetDescription();

        void Initialize(ICascadeContext context);

        DefaultOffCriticalDataPathObserver GetObserver(
            ICascadeContext context, 
            JsonObject.JsonValueType udlConfig);

        void Release();

        void OcdpoHandler();
    }
} // namespace Derecho.Cascade