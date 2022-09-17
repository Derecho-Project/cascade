using System.Collections.Generic;
using System.Text;

namespace WorkerLib
{
    class Device
    {
        public string Type { get; set; }
        public int Id { get; set; }
        public bool IsValid { get; set; }
        public List<string> Properties { get; set; }
        public int Value { get; set; }

        public string AsJsonString
        {
            get
            {
                var sbProps = new StringBuilder();
                for (var i = 0; i < Properties.Count; i++)
                {
                    var ch = i < Properties.Count - 1 ? "," : string.Empty;
                    sbProps.Append($"\"{Properties[i]}\"{ch}");
                }

                var sb = new StringBuilder();
                sb.Append("{");
                sb.Append($"\"type\":\"{Type}\",");
                sb.Append($"\"id\":{Id},");
                sb.Append($"\"valid\":{IsValid.ToString().ToLower()},");
                sb.Append($"\"properties\":[{sbProps.ToString()}],");
                sb.Append($"\"value\":{Value}");
                sb.Append("}");
                return sb.ToString();
            }
        }
    }
}
