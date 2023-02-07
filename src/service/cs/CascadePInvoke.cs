using System;
using System.Runtime.InteropServices;

namespace Derecho.Cascade
{
    public unsafe class CascadePInvoke
    {
        [DllImport("libcascade_client_cs.so")]
        public static extern IntPtr get_service_client_ref();

        [DllImport("libcascade_client_cs.so")]
        public static extern UInt32 get_subgroup_index_vcss(IntPtr capi);

        [DllImport("libcascade_client_cs.so")]
        public static extern UInt32 get_my_id(IntPtr capi);

        public static void Main()
        {
            IntPtr capi = get_service_client_ref();
            var subgroupIndex = get_subgroup_index_vcss(capi);
            var nodeId = get_my_id(capi);

            Console.WriteLine("Subgroup index: " + subgroupIndex);
            Console.WriteLine("Node id: " + nodeId);
        }
    }
} // namespace Derecho.Cascade
