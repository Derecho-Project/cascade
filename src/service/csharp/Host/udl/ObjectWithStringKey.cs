
namespace Derecho.Cascade 
{
    
    public struct ObjectWithStringKey
    {
        public ObjectWithStringKey(string key, System.Object obj)
        {
            Key = key;
            Obj = obj;
        }
        public string Key { get; }
        public System.Object Obj { get; }
    }
}