#!/usr/bin/env python3
import cascade_py

def main1():
    a = cascade_py.ServiceClientAPI()
    print(a.get_members())
    print(a.get_shard_members("VCSU",0,0))
    print(a.get_shard_members("VCSS",0,0))
    print(a.get_shard_members("PCSU",0,0))
    print(a.get_shard_members("PCSS",0,0))
    #print(a.get_shard_members(0,0))
    #print(a.get_shard_members(1,0))
    #print(a.get_shard_members(2,0))
    #print(a.get_shard_members(3,0))

def main2():
    a = cascade_py.ServiceClientAPI()

    help_info = "\
list_all_members\n\tlist all members in top level derecho group.\n\
list_type_members <type> [subgroup_index] [shard_index]\n\tlist members in shard by subgroup type.\n\
list_subgroup_members [subgroup_id] [shard_index]\n\tlist members in shard by subgroup id.\n\
set_member_selection_policy <type> <subgroup_index> <shard_index> <policy> [user_specified_node_id]\n\tset member selection policy\n\
get_member_selection_policy <type> [subgroup_index] [shard_index]\n\tget member selection policy\n\
put <type> <key> <value> [subgroup_index] [shard_index]\n\tput an object\n\
remove <type> <key> [subgroup_index] [shard_index]\n\tremove an object\n\
get <type> <key> [version] [subgroup_index] [shard_index]\n\tget an object(by version)\n\
get_by_time <type> <key> <ts_us> [subgroup_index] [shard_index]\n\tget an object by timestamp\n\
quit|exit\n\texit the client.\n\
help\n\tprint this message.\n\
\n\
type:=VCSU|VCSS|PCSU|PCSS\n\
policy:=FirstMember|LastMember|Random|FixedRandom|RoundRobin|UserSpecified\n\
"

    while(True):
        subgroup_index = 0
        shard_index = 0

        s = input("cmd> ")
        if s == '':
            continue
        sl = s.split()
        if(len(sl) < 1):
            continue
        
        if(sl[0] == 'help'):
            print(help_info)
            continue

        if(sl[0] == "quit" or sl[0] == "exit"):
            break
        
        if(sl[0] == 'list_all_members'):
            print("Top Derecho Group members = ", a.get_members())
            continue
        
        if(sl[0] == 'list_type_members'):

            if(len(sl) < 2):
                print("Invalid format")
                continue
            
            if(len(sl) >= 3):
                subgroup_index = int(sl[2])

            if(len(sl) >= 4):
                shard_index = int(sl[3])

            print(a.get_shard_members(sl[1], subgroup_index, shard_index))

            continue
        
        if(sl[0] == 'list_subgroup_members'):
            if(len(sl) >= 2):
                subgroup_index = int(sl[1])

            if(len(sl) >= 3):
                shard_index = int(sl[2])

            # print(a.get_shard_members(subgroup_index, shard_index))
            print("This feature deprecated because subgroup ID should be hidden from application.")

            continue

        if(sl[0] == 'get_member_selection_policy'):
            if(len(sl) < 2):
                print("Invalid format")
                continue
            
            if(len(sl) >= 3):
                subgroup_index = int(sl[2])

            if(len(sl) >= 4):
                shard_index = int(sl[3])

            print(a.get_member_selection_policy(sl[1], subgroup_index, shard_index))

            continue

        if(sl[0] == 'set_member_selection_policy'):
            if(len(sl) < 5):
                print("Invalid format")
                continue

            policy = ''
            uindex = 0
            
            if(len(sl) >= 3):
                subgroup_index = int(sl[2])

            if(len(sl) >= 4):
                shard_index = int(sl[3])

            if(len(sl) >= 5):
                policy = sl[4]
            
            if(len(sl) >= 6):
                uindex = int(sl[5])

            print(a.set_member_selection_policy(sl[1], subgroup_index, shard_index, policy, uindex))
            continue

        if(sl[0] == 'put'):
            if(len(sl) < 4):
                print("Invalid format")
                continue
            
            if(len(sl) >= 5):
                subgroup_index = int(sl[4])

            if(len(sl) >= 6):
                shard_index = int(sl[5])

            b = a.put(sl[1], sl[2], bytes(sl[3],'utf-8'), subgroup_index, shard_index)
            print(b.get_result())
            continue

        if(sl[0] == 'get'):
            if(len(sl) < 3):
                print("Invalid format")
                continue
            
            version = -1
            
            if(len(sl) >= 4):
                version = int(sl[3])

            if(len(sl) >= 5):
                subgroup_index = int(sl[4])

            if(len(sl) >= 6):
                shard_index = int(sl[5])

            b = a.get(sl[1], sl[2], version, subgroup_index, shard_index)

            print(b.get_result())
            
            continue

        if(sl[0] == 'remove'):
            if(len(sl) < 3):
                print("Invalid format")
                continue
            
            if(len(sl) >= 4):
                subgroup_index = int(sl[3])

            if(len(sl) >= 5):
                shard_index = int(sl[4])

            b = a.remove(sl[1], sl[2], subgroup_index, shard_index)
            print(b.get_result())
            continue
        
        if(sl[0] == 'get_by_time'):
            if(len(sl) < 3):
                print("Invalid format")
                continue
            
            ts_us = 0
            
            if(len(sl) >= 4):
                ts_us = int(sl[3])

            if(len(sl) >= 5):
                subgroup_index = int(sl[4])

            if(len(sl) >= 6):
                shard_index = int(sl[5])

            b = a.get_by_time(sl[1], sl[2], ts_us, subgroup_index, shard_index)
            print(b.get_result())
            continue


if __name__ == "__main__":
    main2()
