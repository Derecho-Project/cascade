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
        Client client = new Client();
        System.out.println(client.getMembers());
        System.out.println(client.getShardMembers(ServiceType.VCSS, 0, 0));
        System.out.println(client.getShardMembers(ServiceType.PCSS, 0, 0));
    }

    /**
     * Translate strings to their service types.
     * 
     * @param str The string to translate.
     * @return The corresponding subgroup type of cascade.
     */
    public static ServiceType stringToType(String str) {
        switch (str) {
            case "VCSS":
                return ServiceType.VCSS;
            case "PCSS":
                return ServiceType.PCSS;
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

    public static String help_info = "" + "list_all_members\n\tlist all members in top level derecho group.\n"
            + "list_type_members <type> [subgroup_index] [shard_index]\n\tlist members in shard by subgroup type.\n"
            + "list_subgroup_members [subgroup_id] [shard_index]\n\tlist members in shard by subgroup id.\n"
            + "set_member_selection_policy <type> <subgroup_index> <shard_index> <policy> [user_specified_node_id]\n\tset member selection policy\n"
            + "get_member_selection_policy <type> [subgroup_index] [shard_index]\n\tget member selection policy\n"
            + "put <type> <key> <value> [subgroup_index] [shard_index]\n\tput an object\n"
            + "remove <type> <key> [subgroup_index] [shard_index]\n\tremove an object\n"
            + "get <type> <key> [version] [subgroup_index] [shard_index]\n\tget an object(by version)\n"
            + "get_by_time <type> <key> <ts_us> [subgroup_index] [shard_index]\n\tget an object by timestamp\n"
            + "quit|exit\n\texit the client.\n" + "help\n\tprint this message.\n" + "\n" + "type:=VCSS|PCSS\n"
            + "policy:=FirstMember|LastMember|Random|FixedRandom|RoundRobin|UserSpecified\n" + "";

    /**
     * An interactive test on whether the client APIs work.
     */
    public static final void main2() {
        Client client = new Client();
        try {
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
                        // TODO
                        System.out.println(help_info);
                        break;
                    case "quit":
                    case "exit":
                        System.exit(0);
                    case "list_all_members":
                        // list all members in the group.
                        System.out.println("Top Derecho Group members = " + client.getMembers());
                        break;
                    case "list_type_members":
                        // list all members in the specified subgroup or shard.
                        if (splited.length < 2) {
                            System.out.println("\u001B[31m" + "Invalid format: " + str);
                            continue;
                        }
                        ServiceType type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println("\u001B[31m" + "Invalid type: " + str);
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
                    case "list_subgroup_members":
                        // list all members in the specified subgroup or shard.
                        if (splited.length >= 2) {
                            subgroupIndex = Long.parseLong(splited[1]);
                        }
                        if (splited.length >= 3) {
                            shardIndex = Long.parseLong(splited[2]);
                        }
                        // System.out.println(client.getShardMembers(subgroupIndex, shardIndex));
                        System.out.println("This feature is deprecated. Subgroup ID should be hidden from application.");
                        break;
                    case "get_member_selection_policy":
                        // get member selection policy in the specified subgroup or shard.
                        if (splited.length < 2) {
                            System.out.println("\u001B[31m" + "Invalid format: " + str);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println("\u001B[31m" + "Invalid type: " + str);
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
                            System.out.println("\u001B[31m" + "Invalid format: " + str);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println("\u001B[31m" + "Invalid type: " + str);
                            continue;
                        }
                        subgroupIndex = Long.parseLong(splited[2]);
                        shardIndex = Long.parseLong(splited[3]);
                        policy = stringToPolicy(splited[4]);
                        if (policy == ShardMemberSelectionPolicy.InvalidPolicy) {
                            System.out.println("\u001B[31m" + "Invalid policy: " + splited[4]);
                            continue;
                        }
                        if (policy == ShardMemberSelectionPolicy.UserSpecified) {
                            if (splited.length >= 6) {
                                policy.setUNode(Integer.parseInt(splited[5]));
                            } else {
                                System.out.println("\u001B[31m" + "Should have specified policy but didn't");
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
                            System.out.println("\u001B[31m" + "Invalid format: " + str);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println("\u001B[31m" + "Invalid type: " + str);
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

                        QueryResults<Bundle> qr = client.put(type, bbkey, bbval, subgroupIndex, shardIndex);
                        System.out.println(qr.get());
                        break;
                    case "remove":
                        // remove a key-value pair from cascade.
                        if (splited.length < 3) {
                            System.out.println("\u001B[31m" + "Invalid format: " + str);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println("\u001B[31m" + "Invalid type: " + str);
                            continue;
                        }
                        if (splited.length >= 4)
                            subgroupIndex = Integer.parseInt(splited[3]);
                        if (splited.length >= 5)
                            shardIndex = Integer.parseInt(splited[4]);
                        arr = splited[2].getBytes();
                        bbkey = ByteBuffer.allocateDirect(arr.length);
                        bbkey.put(arr);
                        qr = client.remove(type, bbkey, subgroupIndex, shardIndex);
                        System.out.println(qr.get());
                        break;
                    case "get":
                        // get a key-value pair from cascade by version. Version would be -1
                        // when not specified.
                        if (splited.length < 3) {
                            System.out.println("\u001B[31m" + "Invalid format: " + str);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println("\u001B[31m" + "Invalid type: " + str);
                            continue;
                        }
                        long version = -1;
                        if (splited.length >= 4)
                            version = Long.parseLong(splited[3]);
                        if (splited.length >= 5)
                            subgroupIndex = Integer.parseInt(splited[4]);
                        if (splited.length >= 6)
                            shardIndex = Integer.parseInt(splited[5]);
                        arr = splited[2].getBytes();
                        bbkey = ByteBuffer.allocateDirect(arr.length);
                        bbkey.put(arr);
                        QueryResults<ByteBuffer> qrb = client.get(type, bbkey, version, subgroupIndex, shardIndex);
                        Map<Integer, ByteBuffer> data = qrb.get();
                        for (ByteBuffer bb : data.values()) {
                            byte b[] = new byte[bb.capacity()];
                            for (int i = 0;i < bb.capacity(); i++){
                                b[i] = bb.get(i);
                            }
                            System.out.println("bytes: " + new String(b));
                        }
                        break;
                    case "get_by_time":
                        // get a key-value pair from cascade by timestamp.
                        if (splited.length < 4) {
                            System.out.println("\u001B[31m" + "Invalid format: " + str);
                            continue;
                        }
                        type = stringToType(splited[1]);
                        if (type == null) {
                            System.out.println("\u001B[31m" + "Invalid type: " + str);
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
                        qrb = client.getByTime(type, bbkey, timestamp, subgroupIndex, shardIndex);
                        data = qrb.get();
                        for (ByteBuffer bb : data.values()) {
                            byte b[] = new byte[bb.capacity()];
                            for (int i = 0;i < bb.capacity(); i++){
                                b[i] = bb.get(i);
                            }
                            System.out.println("bytes: " + new String(b));
                        }
                        break;
                    default:
                        System.out.println("\u001B[31m" + "Command: " + splited[0] + " is not implemented");
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
