using System.Text.Json;

namespace Derecho.Cascade
{
    public class HelloWorldUDL : DefaultOffCriticalDataPathObserver 
    {
        public const string Uuid = "3ce8304e-0b84-44f9-82a4-bd3473782dae";
        public const string Description = "Hello World UDL, prints \"Hello World\" on function call";

        private HelloWorldUDL _udlInstance;

        public string GetUuid()
        {
            return Uuid;
        }

        public string GetDescription()
        {
            return Description;
        }

        public void Initialize(ICascadeContext context)
        {
            if (!_udlInstance) {
                _udlInstance = new _udlInstance();
            }
        }

        public DefaultOffCriticalDataPathObserver GetObserver(ICascadeContext context,
            JsonObject udlConfig)
        {
            return _udlInstance; 
        }

        public void Release()
        {
            // nothing to release
            return;
        }

        public void OcdpoHandler(in NodeId sender, in string objectPoolPathname, in string keyString, 
            in ObjectWithStringKey obj, in Action<string, Object> emit, 
            DefaultCascadeContextType typedCtxt, UInt32 workerId)
        {
            System.Console.WriteLine("Hello, World!");
        }
    }
} // namespace Derecho.Cascade