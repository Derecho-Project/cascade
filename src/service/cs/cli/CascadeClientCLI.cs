using Derecho.Cascade;
using System;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using static Derecho.Cascade.CascadeClient;
using System.Linq;

namespace Derecho.Cascade
{
    // Delegate for a command execution. Should have console printing side effects.
    public delegate void CommandHandler(CascadeClient client, string[] args);

    // Value type for a cascade client command with its arguments.
    public readonly record struct Command(string name, 
                                          string description,
                                          string helpGuidelines,
                                          CommandHandler handler);

    // Custom exception for reporting an invalid subgroup type.
    public class InvalidSubgroupTypeException : Exception
    {
        public InvalidSubgroupTypeException()
        {
        }

        public InvalidSubgroupTypeException(string message)
            : base(message)
        {
        }

        public InvalidSubgroupTypeException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }

    // Custom exception for reporting an invalid command (too few args).
    public class InvalidCommandLengthException : Exception
    {
        public int trueSize { get; }

        public InvalidCommandLengthException(int trueSize)
        {
            this.trueSize = trueSize;
        }

        public InvalidCommandLengthException(string message)
            : base(message)
        {
        }

        public InvalidCommandLengthException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }

    public class CascadeClientCLI
    {
        public static Dictionary<string, SubgroupType> SUBGROUP_TYPE_DICT = 
            new Dictionary<string, SubgroupType>
                {
                    {"VCSS", SubgroupType.VolatileCascadeStoreWithStringKey},
                    {"PCSS", SubgroupType.PersistentCascadeStoreWithStringKey},
                    {"TCSS", SubgroupType.TriggerCascadeNoStoreWithStringKey}
                };

        private static string[] SUBGROUP_TYPE_LIST = SUBGROUP_TYPE_DICT.Keys.ToArray();

        private static string SUBGROUP_TYPE_LIST_STRING = String.Join("|", SUBGROUP_TYPE_LIST);
        
        private static string SHARD_MEMBER_SELECTION_POLICY_LIST = String.Join("|", 
            Enum.GetNames(typeof(ShardMemberSelectionPolicy)));

        private static void CheckFormat(string[] args, int expectedMaxLength)
        {
            if (args.Length > expectedMaxLength)
            {
                throw new InvalidCommandLengthException(expectedMaxLength);
            }
        }

        private static SubgroupType ParseSubgroup(string input)
        {
            if (!SUBGROUP_TYPE_DICT.ContainsKey(input))
            {
                throw new InvalidSubgroupTypeException();
            }
            return SUBGROUP_TYPE_DICT[input];
        }

        private static void PrintSubgroupMembers(List<List<UInt32>> members)
        {
            UInt64 shardIndex = 0;
            Console.Write("-> ");
            foreach (List<UInt32> shard in members)
            {
                Console.Write($"shard-{shardIndex} = [");
                Console.Write(String.Join(", ", shard.ToArray()));
                Console.Write("]");
                Console.WriteLine();
                shardIndex++;
            }
        }

        private static void PrintNodeIdList(List<UInt32> list)
        {
            PrintResult("[" + String.Join(", ", list.ToArray()) + "]");
        }

        private static void PrintStringList(List<string> list)
        {
            PrintResult("[" + String.Join(", ", list.ToArray()) + "]");
        }

        public static Command[] commands = new Command[]
        {
            new Command
            (
                "help",
                "Print help info",
                "help [command name]",
                (_, args) =>
                {
                    CheckFormat(args, 2);
                    if (args.Length >= 2)
                    {
                        Command command = commands.FirstOrDefault(command => command.name.Equals(args[1]));
                        if (command != default(Command))
                        {
                            Console.WriteLine(command.helpGuidelines);
                        }
                        else
                        {
                            PrintRed("Invalid command. Please try again, or use 'help' to list all available commands.");
                        }
                    }
                    else
                    {
                        var commandsWithInstructions = from command in commands
                            where !command.name.Equals("help")
                            select $"{command.name.PadRight(30)} \t\t\t - {command.description}";
                        Console.WriteLine(String.Join(Environment.NewLine, commandsWithInstructions));
                    }
                }
            ),
            new Command(
                "get_my_id",
                "Gets the current ID of the client node.",
                "get_my_id",
                (client, _) =>
                {
                    PrintResult(client.GetMyId().ToString());
                }
            ),
            new Command
            (
                "list_members",
                "List the IDs of all nodes in the Cascade service.",
                "list_members",
                (client, _) =>
                {
                    var members = client.GetMembers();
                    PrintResult("[" + String.Join(", ", members.ToArray()) + "]");
                }
            ),
            new Command
            (
                "list_subgroup_members",
                "List the nodes in a subgroup specified by type and subgroup index.",
                "list_subgroup_members <type> [subgroup index(default:0)]\n" +
                    "type := " + SUBGROUP_TYPE_LIST_STRING,
                (client, args) => 
                {
                    CheckFormat(args, 3);
                    UInt32 subgroupIndex = 0;
                    SubgroupType type = ParseSubgroup(args[1]); 
                    if (args.Length >= 3)
                    {
                        subgroupIndex = UInt32.Parse(args[2]);
                    }
                    var members = client.GetSubgroupMembers(type, subgroupIndex);
                    PrintSubgroupMembers(members);
                }
            ),
            new Command
            (
                "op_list_subgroup_members",
                "List the subgroup members by object pool name.",
                "op_list_subgroup_members <object pool pathname>",
                (client, args) =>
                {
                    CheckFormat(args, 2);
                    string objectPoolPathname = args[1];
                    Console.WriteLine("for object pool: " + args[1]);
                    var members = client.GetSubgroupMembersByObjectPool(objectPoolPathname);
                    PrintSubgroupMembers(members);
                }
            ),
            new Command
            (
                "list_shard_members",
                "List the node IDs in a shard specified by subgroup type, subgroup index, and shard index.",
                "list_shard_members <type> [subgroup index(default:0)] [shard index(default:0)]\n" +
                    "type := " + SUBGROUP_TYPE_LIST_STRING,
                (client, args) =>
                {
                    CheckFormat(args, 4);
                    UInt32 subgroupIndex = 0, shardIndex = 0;
                    SubgroupType type = ParseSubgroup(args[1]);
                    if (args.Length >= 3)
                    {
                        subgroupIndex = UInt32.Parse(args[2]);
                    }
                    if (args.Length >= 4)
                    {
                        shardIndex = UInt32.Parse(args[3]);
                    }
                    var members = client.GetShardMembers(type, subgroupIndex, shardIndex);
                    PrintNodeIdList(members);
                }
            ),
            new Command
            (
                "op_list_shard_members",
                "List the shard members by object pool name.",
                "op_list_shard_members <object pool pathname> [shard index(default:0)]",
                (client, args) =>
                {
                    CheckFormat(args, 3);
                    UInt32 shardIndex = 0;
                    if (args.Length >= 3)
                    {
                        shardIndex = UInt32.Parse(args[2]);
                    }
                    var members = client.GetShardMembersByObjectPool(args[1], shardIndex);
                    PrintNodeIdList(members);
                }
            ),
            new Command
            (
                "set_member_selection_policy",
                "Set the policy for choosing among a set of server members.",
                "set_member_selection_policy <type> <subgroup_index> <shard_index> <policy> [user specified node id]\n" +
                    "type := " + SUBGROUP_TYPE_LIST_STRING + "\n" +
                    "policy := " + SHARD_MEMBER_SELECTION_POLICY_LIST,
                (client, args) =>
                {
                    CheckFormat(args, 6);
                    SubgroupType type = ParseSubgroup(args[1]);
                    UInt32 subgroupIndex = UInt32.Parse(args[2]);
                    UInt32 shardIndex = UInt32.Parse(args[3]);
                    
                   
                    if (Enum.TryParse(args[4], out ShardMemberSelectionPolicy policy))
                    {
                        if (policy == ShardMemberSelectionPolicy.InvalidPolicy)
                        {
                            PrintRed("Policy cannot be InvalidPolicy.");
                        }
                        UInt32 nodeId = 0xffffffff;
                        if (args.Length >= 6)
                        {
                            nodeId = UInt32.Parse(args[5]);
                        }
                        client.SetMemberSelectionPolicy(type, subgroupIndex, shardIndex, policy, nodeId);
                    }
                    else
                    {
                        PrintRed("Invalid policy name: " + args[4]);
                    }
                }
            ),
            new Command
            (
                "get_member_selection_policy",
                "Get the policy for choosing among a set of server members.",
                "get_member_selection_policy <type> <subgroup_index> <shard_index>\n" +
                    "type := " + SUBGROUP_TYPE_LIST_STRING,
                (client, args) =>
                {
                    CheckFormat(args, 4);
                    SubgroupType type = ParseSubgroup(args[1]);
                    UInt32 subgroupIndex = UInt32.Parse(args[2]);
                    UInt32 shardIndex = UInt32.Parse(args[3]);
                    PrintResult(client.GetMemberSelectionPolicy(type, subgroupIndex, shardIndex).ToString());
                }
            ),
            new Command
            (
                "list_object_pools",
                "List existing object pools",
                "list_object_pools",
                (client, args) =>
                {
                    var objectPools = client.ListObjectPools();
                    PrintStringList(objectPools);
                }
            ),
            new Command
            (
                "create_object_pool",
                "Create an object pool",
                "create_object_pool <path> <type> <subgroup_index> [affinity_set_regex]\n" +
                    "type := " + SUBGROUP_TYPE_LIST_STRING,
                (client, args) =>
                {
                    CheckFormat(args, 5);
                    string objectPoolPath = args[1];
                    SubgroupType type = ParseSubgroup(args[2]);
                    UInt32 subgroupIndex = UInt32.Parse(args[3]);
                    string affinitySetRegex = "";
                    if (args.Length >= 5)
                    {
                        affinitySetRegex = args[4];
                    }
                    var versionTimestamp = client.CreateObjectPool(objectPoolPath, type, subgroupIndex, affinitySetRegex);
                    PrintResult(versionTimestamp.ToString());
                }
            ),
            new Command
            (
                "remove_object_pool",
                "Soft-remove an object pool.",
                "remove_object_pool <path>",
                (client, args) =>
                {
                    CheckFormat(args, 2);
                    var versionTimestamp = client.RemoveObjectPool(args[1]);
                    PrintResult(versionTimestamp.ToString());
                }
            ),
            new Command
            (
                "get_object_pool",
                "Get details of an object pool.",
                "get_object_pool <path>",
                (client, args) =>
                {
                    CheckFormat(args, 2);
                    var opm = client.GetObjectPool(args[1]);
                    PrintResult(opm.ToString());
                }
            ),
            new Command
            (
                "put",
                "Put an object to a shard.",
                "put <type> <key> <value> <subgroup_index> <shard_index> [previous_version(default:-1)] [previous_version_by_key(default:-1)]\n" +
                    "type := " + SUBGROUP_TYPE_LIST_STRING,
                (client, args) =>
                {
                    CheckFormat(args, 8);
                    SubgroupType type = ParseSubgroup(args[1]);
                    string key = args[2];
                    string value = args[3];
                    byte[] bytes = Encoding.ASCII.GetBytes(value);
                    UInt32 subgroupIndex = UInt32.Parse(args[4]);
                    UInt32 shardIndex = UInt32.Parse(args[5]);
                    Int64 previousVersion = -1L;
                    Int64 previousVersionByKey = -1L; 
                    if (args.Length >= 7)
                    {  
                        previousVersion = Int64.Parse(args[6]);
                    }
                    if (args.Length >= 8)
                    {
                        previousVersionByKey = Int64.Parse(args[7]);
                    }
                    var versionTimestamp = client.Put(key, bytes, type, subgroupIndex, shardIndex, previousVersion, previousVersionByKey);
                    PrintResult(versionTimestamp.ToString()); 
                }
            ),
            new Command
            (
                "op_put",
                "Put an object into an object pool.",
                "op_put <key> <value> [previous_version(default:-1)] [previous_version_by_key(default:-1)]\n" +
                "Please note that Cascade automatically decides the object pool path using the key's prefix.",
                (client, args) =>
                {
                    CheckFormat(args, 5);
                    Int64 previousVersion = -1L;
                    Int64 previousVersionByKey = -1L;
                    string key = args[1];
                    string value = args[2];
                    byte[] bytes = Encoding.ASCII.GetBytes(value);
                    var versionTimestamp = client.Put(key, bytes, previousVersion: previousVersion, previousVersionByKey: previousVersionByKey);
                    PrintResult(versionTimestamp.ToString());
                }
            ),
            new Command
            (
                "get",
                "Get an object (by version)",
                "get <type> <key> <stable> <subgroup_index> <shard_index> [ version(default:current version) ]\n" +
                    "type := " + SUBGROUP_TYPE_LIST_STRING + "\n" + 
                    "stable := 1|0  using stable data or not.",
                (client, args) =>
                {
                    CheckFormat(args, 7);
                    SubgroupType type = ParseSubgroup(args[1]);
                    string key = args[2];
                    bool stable = args[3].Equals("1");
                    UInt32 subgroupIndex = UInt32.Parse(args[4]);
                    UInt32 shardIndex = UInt32.Parse(args[5]);
                    Int64 version = -1L;
                    if (args.Length >= 7)
                    {
                        version = Int64.Parse(args[6]);
                    }
                    var objectProperties = client.Get(key, type, subgroupIndex, shardIndex, version, stable);
                    PrintResult(objectProperties.ToString());
                }
            ),
            new Command
            (
                "op_get",
                "Get an object from an object pool (by version).",
                "op_get <key> <stable> [ version(default:current version) ]\n" +
                    "stable := 1|0  using stable data or not.\n" +
                    "Please note that Cascade automatically decides the object pool path using the key's prefix.",
                (client, args) =>
                {
                    CheckFormat(args, 4);
                    string key = args[1];
                    bool stable = args[2].Equals("1");
                    Int64 version = -1L;
                    if (args.Length >= 4)
                    {
                        version = Int64.Parse(args[3]);
                    }
                    var objectProperties = client.Get(key, stable: stable, version: version);
                    PrintResult(objectProperties.ToString());
                }
            ),
            new Command
            (
                "remove",
                "Remove an object from a shard.",
                "remove <type> <key> <subgroup_index> <shard_index> \n" +
                     "type := " + SUBGROUP_TYPE_LIST_STRING,
                (client, args) =>
                {
                    CheckFormat(args, 5);
                    SubgroupType type = ParseSubgroup(args[1]);
                    string key = args[2];
                    UInt32 subgroupIndex = UInt32.Parse(args[3]);
                    UInt32 shardIndex = UInt32.Parse(args[4]);
                    var versionTimestamp = client.Remove(key, type, subgroupIndex, shardIndex);
                    PrintResult(versionTimestamp.ToString());
                }
            ),
            new Command
            (
                "op_remove",
                "Remove an object from an object pool.",
                "op_remove <key>\n" +
                    "Please note that cascade automatically decides the object pool path using the key's prefix.",
                (client, args) =>
                {
                    CheckFormat(args, 2);
                    string key = args[1];
                    var versionTimestamp = client.Remove(key);
                    PrintResult(versionTimestamp.ToString());
                }
            ),
            new Command
            (
                "list_keys",
                "List the object keys in a shard (by version).",
                "list_keys <type> <stable> <subgroup_index> <shard_index> [ version(default:current version) ]\n" +
                    "type := " + SUBGROUP_TYPE_LIST_STRING + "\n" + 
                    "stable := 1|0  using stable data or not.",
                (client, args) =>
                {
                    CheckFormat(args, 6);
                    SubgroupType type = ParseSubgroup(args[1]);
                    bool stable = args[2].Equals("1");
                    UInt32 subgroupIndex = UInt32.Parse(args[3]);
                    UInt32 shardIndex = UInt32.Parse(args[4]);
                    Int64 version = -1L;
                    if (args.Length >= 6)
                    {
                        version = Int64.Parse(args[5]);
                    }

                    var list = client.ListKeysInShard(type, subgroupIndex, shardIndex, version, stable);
                    PrintStringList(list);
                }
            ),
            new Command
            (
                "op_list_keys",
                "list the object keys in an object pool (by version).",
                "op_list_keys <object pool pathname> <stable> [ version(default:current version) ]\n" +
                     "stable := 1|0  using stable data or not.",
                (client, args) =>
                {
                    CheckFormat(args, 4);
                    string objectPoolPath = args[1];
                    bool stable = args[2].Equals("1");
                    Int64 version = -1L;
                    if (args.Length >= 4)
                    {
                        version = Int64.Parse(args[3]);
                    }
                    var list = client.ListKeysInObjectPool(objectPoolPath, version, stable);
                    PrintStringList(list);
                }
            ),
            new Command
            (
                "from_shard",
                "Create an iterator to all the objects within a shard and list their metadata.",
                "from_shard <type> <subgroup_index> <shard_index> [ version(default:current version) ]" +
                    "type := " + SUBGROUP_TYPE_LIST_STRING,
                (client, args) =>
                {
                    CheckFormat(args, 5);
                    SubgroupType type = ParseSubgroup(args[1]);
                    UInt32 subgroupIndex = UInt32.Parse(args[2]);
                    UInt32 shardIndex = UInt32.Parse(args[3]);
                    Int64 version = -1L;

                    if (args.Length >= 5)
                    {
                        version = Int64.Parse(args[4]);
                    }

                    var iterator = client.FromShard(type, subgroupIndex, shardIndex, version);
                    foreach (var obj in iterator)
                    {
                        Console.WriteLine(obj.ToString());
                    }
                }
            )
        };

        public static void Main(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Welcome to the Cascade C# Client CLI.");
            Console.ResetColor();
            Console.WriteLine("Loading client service ref instance...");
            CascadeClient client = new CascadeClient();
            Console.WriteLine("Done.");

            string command;
            while (true)
            {
                Console.Write("cmd> ");
                command = Console.ReadLine();
                string[] parts = Array.ConvertAll(command.Split(' '), p => p.Trim());
                string name = parts[0].ToLower();
                if (parts[0].Equals("quit"))
                {
                    Console.WriteLine("Client exits.");
                    break;
                }
                handleCommand(parts[0].ToLower(), client, parts);
            }
        }

        private static void handleCommand(string name, CascadeClient client, string[] args) {
            Command command = commands.FirstOrDefault(command => command.name.Equals(name));
            if (command == default(Command))
            {
                PrintRed("Invalid command. Please try again, or use 'help' to list all available commands.");
                return;
            }
            try
            {
                command.handler(client, args);
                // if execution reaches this line, an exception was not thrown.
                Console.WriteLine("-> Succeeded.");
            }
            catch (InvalidSubgroupTypeException)
            {
                PrintRed($"Invalid subgroup type entered. Please use one of type := {SUBGROUP_TYPE_LIST_STRING}");
            }
            catch (InvalidCommandLengthException exception)
            {
                PrintRed($"Invalid number of arguments supplied. For this command, a maximum of {exception.trueSize - 1} arguments are expected.");
            }
            catch (Exception exception)
            {
                PrintRed("An error occurred when handling your command. Please try again or file an issue in the repo if it persists.");
                Console.WriteLine(exception.ToString());
            }
        }

        private static void PrintResult(string result)
        {
            Console.WriteLine("-> " + result);
        }

        private static void PrintRed(string str)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(str);
            Console.ResetColor();
        }
    } 
} // namespace Derecho.Cascade
