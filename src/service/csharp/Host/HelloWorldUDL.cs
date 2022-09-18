using System.Collections.Generic;  

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

        public static void StaticVoidEntryPoint()
        {
            System.Console.WriteLine("Static void method has been called.");
        }

        public void Initialize(ICascadeContext context)
        {
            if (_udlInstance != null) {
                _udlInstance = new HelloWorldUDL();
            }
        }

        public DefaultOffCriticalDataPathObserver GetObserver(ICascadeContext context,
            Dictionary<string, System.Object> udlConfig)
        {
            return _udlInstance; 
        }

        public void Release()
        {
            // nothing to release
            return;
        }

        public void OcdpoHandler(System.Int64 sender, string objectPoolPathname, string keyString, 
            ObjectWithStringKey obj, System.Action<string, System.Object> emit, 
            DefaultCascadeContextType typedCtxt, System.UInt32 workerId)
        {
            System.Console.WriteLine("Hello, World!");
        }
    }
} // namespace Derecho.Cascade