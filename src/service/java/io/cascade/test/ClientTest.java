package io.cascade.test;

import io.cascade.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Test the use of client.
 */
public class ClientTest {

    /** First test on whether we can get members and get shard members. */
    public static final void main1() {
        try(Client client = new Client()) {
            System.out.println(client.getMembers());
            System.out.println(client.getShardMembers(ServiceType.VolatileCascadeStoreWithStringKey, 0, 0));
            System.out.println(client.getShardMembers(ServiceType.PersistentCascadeStoreWithStringKey, 0, 0));
        }
    }

    /**
     * Translate strings to their service types.
     * 
     * @param str The string to translate.
     * @return The corresponding subgroup type of cascade.
     */
    public static ServiceType stringToType(String str) {
        switch (str) {
            case "VolatileCascadeStoreWithStringKey":
                return ServiceType.VolatileCascadeStoreWithStringKey;
            case "PersistentCascadeStoreWithStringKey":
                return ServiceType.PersistentCascadeStoreWithStringKey;
            case "TriggerCascadeNoStoreWithStringKey":
                return ServiceType.TriggerCascadeNoStoreWithStringKey;
            default:
                return null;
        }
    }

    /**
     * Translate strings to their corresponding shard member selection policies.
     * 
     * @param str The string to translate.
     * @return The corresponding shard member selection policy of cascade.
     */
    public static ShardMemberSelectionPolicy stringToPolicy(String str) {
        switch (str) {
            case "FirstMember":
                return ShardMemberSelectionPolicy.FirstMember;
            case "FixedRandom":
                return ShardMemberSelectionPolicy.FixedRandom;
            case "InvalidPolicy":
                return ShardMemberSelectionPolicy.InvalidPolicy;
            case "LastMember":
                return ShardMemberSelectionPolicy.LastMember;
            case "Random":
                return ShardMemberSelectionPolicy.Random;
            case "RoundRobin":
                return ShardMemberSelectionPolicy.RoundRobin;
            case "UserSpecified":
                return ShardMemberSelectionPolicy.UserSpecified;
            default:
                return ShardMemberSelectionPolicy.InvalidPolicy;
        }
    }

    public static String help_info = "" 
            + "list_all_members\n\tlist all members in top level derecho group.\n"
            + "list_shard_members <type> [subgroup_index] [shard_index]\n\tlist members in shard by subgroup type.\n"
            + "set_member_selection_policy <type> <subgroup_index> <shard_index> <policy> [user_specified_node_id]\n\tset member selection policy\n"
            + "get_member_selection_policy <type> [subgroup_index] [shard_index]\n\tget member selection policy\n"
            + "put <type> <key> <value> [subgroup_index] [shard_index]\n\tput an object\n"
            + "remove <type> <key> [subgroup_index] [shard_index]\n\tremove an object\n"
            + "get <type> <key> <stable> [version] [subgroup_index] [shard_index]\n\tget an object(by version)\n"
            + "get_by_time <type> <key> <ts_us> [subgroup_index] [shard_index]\n\tget an object by timestamp\n"
            + "quit|exit\n\texit the client.\n" + "help\n\tprint this message.\n" 
            + "\n" 
            + "type:=VolatileCascadeStoreWithStringKey|PersistentCascadeStoreWithStringKey|TriggerCascadeNoStoreWithStringKey\n"
            + "policy:=FirstMember|LastMember|Random|FixedRandom|RoundRobin|UserSpecified\n"
            + "stable:=True:False\n";

    /**
     * An interactive test on whether the client APIs work.
     */
    public static final void main2() {
        final String ANSI_RESET = "\u001B[0m";
        final String ANSI_RED   = "\u001B[31m";

        try (Client client = new Client()) {
            while (true) {
                long subgroupIndex = 0, shardIndex = 0;
                System.out.print("cmd> ");
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                String str = br.readLine();
                if (str == null)
                    break;
                String[] splited = str.split("\\s+");
                if (splited.length < 1)
                    continue;
                switch (splited[0]) {
                    case "help":
                        System.out.println(help_info);
                        break;
                    case "quit":
                    case "exit":
                        System.exit(0);
                        break;
                    case "list_all_members":
                        // list all members in the group.
                        System.out.println("Top Derecho Group members = " + client.getMembers());
                        break;
                    case "list_shard_members":
                        // list all members in the specified subgroup or shard.
                        if (splited.length < 2) {
                            System.out.println(ANSI_RED + "Invalid format: " + str + ANSI_RESET);
                            continue;
                        }
                        ServiceType type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println(ANSI_RED + "Invalid type: " + str + ANSI_RESET);
                            continue;
                        }
                        if (splited.length >= 3) {
                            subgroupIndex = Long.parseLong(splited[2]);
                        }
                        if (splited.length >= 4) {
                            shardIndex = Long.parseLong(splited[3]);
                        }
                        System.out.println(client.getShardMembers(type, subgroupIndex, shardIndex));
                        break;
                    case "get_member_selection_policy":
                        // get member selection policy in the specified subgroup or shard.
                        if (splited.length < 2) {
                            System.out.println(ANSI_RED + "Invalid format: " + str + ANSI_RESET);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println(ANSI_RED + "Invalid type: " + str + ANSI_RESET);
                            continue;
                        }
                        if (splited.length >= 3) {
                            subgroupIndex = Long.parseLong(splited[2]);
                        }
                        if (splited.length >= 4) {
                            shardIndex = Long.parseLong(splited[3]);
                        }
                        ShardMemberSelectionPolicy policy = client.getMemberSelectionPolicy(type, subgroupIndex,
                                shardIndex);

                        System.out.println(policy + " " + policy.getUNode());
                        break;
                    case "set_member_selection_policy":
                        // set member selection policy in the specified subgroup or shard.
                        if (splited.length < 5) {
                            System.out.println(ANSI_RED + "Invalid format: " + str + ANSI_RESET);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println(ANSI_RED + "Invalid type: " + str + ANSI_RESET);
                            continue;
                        }
                        subgroupIndex = Long.parseLong(splited[2]);
                        shardIndex = Long.parseLong(splited[3]);
                        policy = stringToPolicy(splited[4]);
                        if (policy == ShardMemberSelectionPolicy.InvalidPolicy) {
                            System.out.println(ANSI_RED + "Invalid policy: " + splited[4] + ANSI_RESET);
                            continue;
                        }
                        if (policy == ShardMemberSelectionPolicy.UserSpecified) {
                            if (splited.length >= 6) {
                                policy.setUNode(Integer.parseInt(splited[5]));
                            } else {
                                System.out.println(ANSI_RED + "Should have specified policy but didn't" + ANSI_RESET);
                                continue;
                            }
                        }
                        client.setMemberSelectionPolicy(type, subgroupIndex, shardIndex, policy);

                        break;
                    // for all following operations, the default subgroup index and shard index
                    // would be 0.
                    case "put":
                        // put a key value pair into cascade.
                        if (splited.length < 4) {
                            System.out.println(ANSI_RED + "Invalid format: " + str + ANSI_RESET);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println(ANSI_RED + "Invalid type: " + str + ANSI_RESET);
                            continue;
                        }
                        if (splited.length >= 5)
                            subgroupIndex = Integer.parseInt(splited[4]);
                        if (splited.length >= 6)
                            shardIndex = Integer.parseInt(splited[5]);
                        byte[] arr = splited[2].getBytes();
                        ByteBuffer bbkey = ByteBuffer.allocateDirect(arr.length);
                        bbkey.put(arr);
                        byte[] arr2 = splited[3].getBytes();
                        ByteBuffer bbval = ByteBuffer.allocateDirect(arr2.length);
                        bbval.put(arr2);

                        try (QueryResults<Bundle> qr = client.put(type, bbkey, bbval, subgroupIndex, shardIndex)) {
                            System.out.println(qr.get());
                        }
                        break;
                    case "remove":
                        // remove a key-value pair from cascade.
                        if (splited.length < 3) {
                            System.out.println(ANSI_RED + "Invalid format: " + str + ANSI_RESET);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println(ANSI_RED + "Invalid type: " + str + ANSI_RESET);
                            continue;
                        }
                        if (splited.length >= 4)
                            subgroupIndex = Integer.parseInt(splited[3]);
                        if (splited.length >= 5)
                            shardIndex = Integer.parseInt(splited[4]);
                        arr = splited[2].getBytes();
                        bbkey = ByteBuffer.allocateDirect(arr.length);
                        bbkey.put(arr);
                        try (QueryResults<Bundle> qr = client.remove(type, bbkey, subgroupIndex, shardIndex)) {
                            System.out.println(qr.get());
                        }
                        break;
                    case "get":
                        // get a key-value pair from cascade by version. Version would be -1
                        // when not specified.
                        if (splited.length < 4) {
                            System.out.println(ANSI_RED + "Invalid format: " + str + ANSI_RESET);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println(ANSI_RED + "Invalid type: " + str + ANSI_RESET);
                            continue;
                        }
                        boolean stable = Boolean.parseBoolean(splited[3]);
                        long version = -1;
                        if (splited.length >= 5)
                            version = Long.parseLong(splited[4]);
                        if (splited.length >= 6)
                            subgroupIndex = Integer.parseInt(splited[5]);
                        if (splited.length >= 7)
                            shardIndex = Integer.parseInt(splited[6]);
                        arr = splited[2].getBytes();
                        bbkey = ByteBuffer.allocateDirect(arr.length);
                        bbkey.put(arr);
                        try (QueryResults<CascadeObject> qrb = client.get(type, bbkey, version, stable, subgroupIndex, shardIndex)) {
                            Map<Integer, CascadeObject> data = qrb.get();
                            for (CascadeObject obj : data.values()) {
                                ByteBuffer bb = obj.object;
                                byte b[] = new byte[bb.capacity()];
                                for (int i = 0;i < bb.capacity(); i++){
                                    b[i] = bb.get(i);
                                }
                                System.out.println("bytes: " + new String(b));
                            }
                        }
                        break;
                    case "get_by_time":
                        // get a key-value pair from cascade by timestamp.
                        if (splited.length < 4) {
                            System.out.println(ANSI_RED + "Invalid format: " + str + ANSI_RESET);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println(ANSI_RED + "Invalid type: " + str + ANSI_RESET);
                            continue;
                        }
                        long timestamp = Long.parseLong(splited[3]);
                        if (splited.length >= 5)
                            subgroupIndex = Integer.parseInt(splited[4]);
                        if (splited.length >= 6)
                            shardIndex = Integer.parseInt(splited[5]);
                        arr = splited[2].getBytes();
                        bbkey = ByteBuffer.allocateDirect(arr.length);
                        bbkey.put(arr);
                        try (QueryResults<CascadeObject> qrb = client.getByTime(type, bbkey, timestamp, subgroupIndex, shardIndex)) {
                            Map<Integer, CascadeObject> data = qrb.get();
                            for (CascadeObject obj : data.values()) {
                                ByteBuffer bb = obj.object;
                                byte b[] = new byte[bb.capacity()];
                                for (int i = 0;i < bb.capacity(); i++){
                                    b[i] = bb.get(i);
                                }
                                System.out.println("bytes: " + new String(b));
                            }
                        }
                        break;
                    default:
                        System.out.println(ANSI_RED + "Command: " + splited[0] + " is not implemented" + ANSI_RESET);
                }
            }
        } catch (IOException e) {
            System.exit(-1);
        }
        System.out.println("Client exits.");
    }

    public static final void main(String[] args) {
        System.out.println("Here is client test!");
        // main1();
        main2();
    }
}
