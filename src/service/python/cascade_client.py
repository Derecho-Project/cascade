#!/usr/bin/env python3
import cmd, sys
import logging
from derecho.cascade.external_client import ServiceClientAPI
from derecho.cascade.external_client import TimestampLogger

class bcolors:
    OK = '\033[92m' #GREEN
    WARNING = '\033[93m' #YELLOW
    FAIL = '\033[91m' #RED
    RESET = '\033[0m' #RESET COLOR

class CascadeClientShell(cmd.Cmd):
    '''
    Cascade Client Shell
    '''
    def __init__(self, name = 'cascade.client'):
        super(CascadeClientShell,self).__init__()
        self.prompt = '(' + name + ') '
        self.capi = None
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.WARN)

    def check_capi(self):
        if self.capi is None:
            self.capi = ServiceClientAPI()

    # commands
    def do_quit(self, arg):
        '''
        Quit Cascade console
        '''
        print('Quitting Cascade Client Shell')
        return True

    def do_list_members(self, arg):
        '''
        list_members
        ============
        List the member nodes in the service by subgroup_type, subgroup, or shard.

        ** optional keyword arguments **
        subgroup_type:  the subgroup type, could be either of the following:
                        VolatileCascadeStoreWithStringKey
                        PersistentCascadeStoreWithStringKey
                        TriggerCascadeNoStoreWithStringKey
        subgroup_index: the subgroup index, default to 0 if subgroup_type is specified
        shard_index:    the shard index
        object_pool_pathname:
                        the object pool name
                        Please note that you should either specify object_pool_pathname or 
                        a (subgroup_type,subgroup_index) pair.
        '''
        self.check_capi()
        subgroup_type = None
        subgroup_index = None
        shard_index = None
        object_pool_pathname = None
        args = arg.split()
        argpos = 0
        while argpos < len(args):
            extra_option = args[argpos].split('=')
            if len(extra_option) != 2:
                print(bcolors.FAIL + "Unknown argument:" + args[2] + bcolors.RESET)
                return
            elif extra_option[0] == 'subgroup_type':
                subgroup_type = extra_option[1]
            elif extra_option[0] == 'subgroup_index':
                subgroup_index = int(extra_option[1],0)
            elif extra_option[0] == 'shard_index':
                shard_index = int(extra_option[1],0)
            elif extra_option[0] == 'object_pool_pathname':
                object_pool_pathname = extra_option[1]
            argpos = argpos + 1
        if object_pool_pathname is None and subgroup_type is None and subgroup_index is None:
            # list all nodes
            print("Nodes in Cascade service:%s" % str(self.capi.get_members()))
            return

        if object_pool_pathname is not None and (subgroup_type is not None or subgroup_index is not None):
            print("Either object_pool_pathname or (subgroup_type,subgroup_index) pair can be specified")
            return

        if object_pool_pathname is None:
            # specified by subgroup_type and subgroup_index
            if subgroup_index is None:
                # list all subgroups
                for subgroup_index in range(self.capi.get_number_of_subgroups(subgroup_type)):
                    print(f"Nodes in subgroup ({subgroup_type}:{subgroup_index}):{str(self.capi.get_subgroup_members(subgroup_type,subgroup_index))}")
            elif shard_index is None:
                # list subgroup members
                print(f"Nodes in subgroup ({subgroup_type}:{subgroup_index}):{str(self.capi.get_subgroup_members(subgroup_type,subgroup_index))}")
            else:
                # list shard members:
                print("Nodes in shard (%s:%d:%d):%s" % (subgroup_type,subgroup_index,shard_index,str(self.capi.get_shard_members(subgroup_type,subgroup_index,shard_index))))
        else:
            # specified by object pool
            if shard_index is None:
                # list subgroup members
                print(f"Nodes in object pool {object_pool_pathname}:{self.capi.get_subgroup_members_by_object_pool(object_pool_pathname)}")
            else:
                # list shard members
                print(f"Node in shard-{shard_index} of object pool {object_pool_pathname}:{self.capi.get_shard_members_by_object_pool(object_pool_pathname,shard_index)}")

    def do_set_member_selection_policy(self, arg):
        '''
        set_member_selection_policy <subgroup_type> <subgroup_index> <shard_index> <policy> [node_id]
        ===========================
        Set member selection policy for a shard

        subgroup_type:  the subgroup type, could be either of the following:
                        VolatileCascadeStoreWithStringKey
                        PersistentCascadeStoreWithStringKey
                        TriggerCascadeNoStoreWithStringKey
        subgroup_index: the subgroup index
        shard_index:    the shard index
        policy:         the member selection policy, could be either of the following:
                        FirstMember
                        LastMember
                        Random
                        FixedRandom
                        RoundRobin
                        KeyHashing
                        UserSpecified
        node_id:        if policy is 'UserSpecified', you need to specify the corresponding node id.
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 4:
            print(bcolors.FAIL + "At least four arguments are required." + bcolors.RESET)
        else:
            subgroup_type = args[0]
            subgroup_index = int(args[1],0)
            shard_index = int(args[2],0)
            policy = args[3]
            node_id = 0
            if policy == 'UserSpecified':
                if len(args) < 5:
                    print(bcolors.FAIL + "Please specify the user-specified node id." + bcolors.RESET)
                    return
                else:
                    node_id = int(args[4],0)
            self.capi.set_member_selection_policy(subgroup_type,subgroup_index,shard_index,policy,node_id)

    def do_get_member_selection_policy(self, arg):
        '''
        get_member_selection_policy <subgroup_type> <subgroup_index> <shard_index>
        ===========================
        Get member selection policy for a shard

        subgroup_type:  the subgroup type, could be either of the following:
                        VolatileCascadeStoreWithStringKey
                        PersistentCascadeStoreWithStringKey
                        TriggerCascadeNoStoreWithStringKey
        subgroup_index: the subgroup index
        shard_index:    the shard index
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 3:
            print(bcolors.FAIL + "At least three arguments are required." + bcolors.RESET)
        else:
            subgroup_type = args[0]
            subgroup_index = int(args[1],0)
            shard_index = int(args[2],0)
            policy,node_id = self.capi.get_member_selection_policy(subgroup_type,subgroup_index,shard_index)
            print(bcolors.OK + f"{policy}:{node_id}" + bcolors.RESET)

    def do_put(self, arg):
        '''
        put <key> <value> [subgroup_type= subgroup_index= shard_index= previous_version= previous_version_by_key= message_id=]
        ===
        Put an object to a shard, either blocking or non-blocking until it's done.

        key:            the key string
        value:          the value string

        ** optional keyword arguments **
        subgroup_type:              optional subgroup type, could be either of the following:
                                    VolatileCascadeStoreWithStringKey
                                    PersistentCascadeStoreWithStringKey
                                    TriggerCascadeNoStoreWithStringKey
                                    by default, put try to find object pool by parsing key
        subgroup_index:             the subgroup index
        shard_index:                the shard index
        previous_version:           optional previous_version, which is checked against the latest version. The value
                                    will get rejected if the latest version grows beyond previous_version.
        previous_version_by_key:    optional previous_version_by_key, which is checked against the latest version of
                                    the key. The value will get rejected if the latest version of the key grows beyond
                                    previous_version.
        message_id:                 The message_id for the object.
        blocking:                   optional blocking flag. Default to True.
        trigger:                    optional trigger flag, Default to False.
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 2:
            print(bcolors.FAIL + "At least two arguments are required." + bcolors.RESET)
        else:
            subgroup_type = ""
            subgroup_index = 0
            shard_index = 0
            previous_version = ServiceClientAPI.CURRENT_VERSION
            previous_version_by_key = ServiceClientAPI.CURRENT_VERSION
            message_id = 0
            blocking = True
            trigger = False
            argpos = 2
            while argpos < len(args):
                extra_option = args[argpos].split('=')
                if len(extra_option) != 2:
                    print(bcolors.FAIL + "Unknown argument:" + args[2] + bcolors.RESET)
                    return
                elif extra_option[0] == 'subgroup_type':
                    subgroup_type = extra_option[1]
                elif extra_option[0] == 'subgroup_index':
                    subgroup_index = int(extra_option[1],0)
                elif extra_option[0] == 'shard_index':
                    shard_index = int(extra_option[1],0)
                elif extra_option[0] == 'previous_version':
                    previous_version = int(extra_option[1],0)
                elif extra_option[0] == 'previous_version_by_key':
                    previous_version_by_key = int(extra_option[1],0)
                elif extra_option[0] == 'message_id':
                    message_id = int(extra_option[1],0)
                elif extra_option[0] == 'blocking' and ( extra_option[1].lower() == 'no' or extra_option[1].lower() == 'false' or extra_option[1].lower() == 'off' or extra_option[1].lower() == '0'  ):
                    blocking = False
                elif extra_option[0] == 'trigger' and ( extra_option[1].lower() == 'yes' or extra_option[1].lower() == 'true' or extra_option[1].lower() == 'on' or extra_option[1].lower() == '1'  ):
                    trigger = True
                argpos = argpos + 1
            res = self.capi.put(args[0],bytes(args[1],'utf-8'),subgroup_type=subgroup_type,subgroup_index=subgroup_index,shard_index=shard_index,previous_version=previous_version,previous_version_by_key=previous_version_by_key,message_id=message_id,blocking=blocking,trigger=trigger)
            if blocking and not trigger and res:
                print(bcolors.OK + f"{res.get_result()}" + bcolors.RESET)
            elif trigger and not res:
                print(bcolors.OK + "trigger put request is sent." + bcolors.RESET)
            elif not blocking and not res:
                print(bcolors.OK + "non-blocking put request is sent." + bcolors.RESET)
            else:
                print(bcolors.FAIL + f"Some unknown error happened:result={res}." + bcolors.RESET)

    def do_remove(self, arg):
        '''
        remove <key> [subgroup_type= subgroup_index= shard_index=]
        ===
        Remove an object from a shard

        key:            the key string

        ** optional keyword arguments **
        subgroup_type:              optional subgroup type, could be either of the following:
                                    VolatileCascadeStoreWithStringKey
                                    PersistentCascadeStoreWithStringKey
                                    TriggerCascadeNoStoreWithStringKey
                                    by default, put try to find object pool by parsing key
        subgroup_index:             the subgroup index
        shard_index:                the shard index
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 1:
            print(bcolors.FAIL + "At least one argument is required." + bcolors.RESET)
        else:
            subgroup_type = ""
            subgroup_index = 0
            shard_index = 0
            argpos = 1
            while argpos < len(args):
                extra_option = args[argpos].split('=')
                if len(extra_option) != 2:
                    print(bcolors.FAIL + "Unknown argument:" + args[2] + bcolors.RESET)
                    return
                elif extra_option[0] == 'subgroup_type':
                    subgroup_type = extra_option[1]
                elif extra_option[0] == 'subgroup_index':
                    subgroup_index = int(extra_option[1],0)
                elif extra_option[0] == 'shard_index':
                    shard_index = int(extra_option[1],0)
                argpos = argpos + 1
            res = self.capi.remove(args[0],subgroup_type=subgroup_type,subgroup_index=subgroup_index,shard_index=shard_index)
            if res:
                print(bcolors.OK + f"{res.get_result()}" + bcolors.RESET)
            else:
                print(bcolors.FAIL + "Something went wrong, remove returns null." + bcolors.RESET)

    def do_get(self, arg):
        '''
        get <key> [subgroup_type= subgroup_index= shard_index= stable= version= timestamp=]
        ===
        Get an Object

        key:            the key string

        *** optional key arguments ***
        subgroup_type:              optional subgroup type, could be either of the following:
                                    VolatileCascadeStoreWithStringKey
                                    PersistentCascadeStoreWithStringKey
                                    TriggerCascadeNoStoreWithStringKey
                                    by default, put try to find object pool by parsing key
        subgroup_index:             the subgroup index
        shard_index:                the shard index
        stable:                     If this is a stable read or not, default to stable.
        version:                    the version. For versioned get only
        timestamp:                  the unix EPOCH timestamp in microseconds. For timestamped-get only
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 1:
            print(bcolors.FAIL + 'At least one argument is required.' + bcolors.RESET)
        else:
            subgroup_type = ""
            subgroup_index = 0
            shard_index = 0
            stable = True
            version = ServiceClientAPI.CURRENT_VERSION
            timestamp = 0
            argpos = 1
            while argpos < len(args):
                extra_option = args[argpos].split('=')
                if len(extra_option) != 2:
                    print(bcolors.FAIL + "Unknown argument:" + args[2] + bcolors.RESET)
                    return
                elif extra_option[0] == 'subgroup_type':
                    subgroup_type = extra_option[1]
                elif extra_option[0] == 'subgroup_index':
                    subgroup_index = int(extra_option[1],0)
                elif extra_option[0] == 'shard_index':
                    shard_index = int(extra_option[1],0)
                elif extra_option[0] == 'stable':
                    if extra_option[1].lower == 'false' or extra_option[1].lower == 'no' or extra_option[1].lower() == 'off' or extra_option[1].lower() == '0':
                        stable = False;
                elif extra_option[0] == 'version':
                    version = int(extra_option[1],0)
                elif extra_option[0] == 'timestamp':
                    timestamp = int(extra_option[1],0)
                argpos = argpos + 1
            res = self.capi.get(args[0],subgroup_type=subgroup_type,subgroup_index=subgroup_index,shard_index=shard_index,version=version,timestamp=timestamp)
            if res:
                odict = res.get_result()
                print(bcolors.OK + f"{str(odict)}" + bcolors.RESET)
            else:
                print(bcolors.FAIL + "Something went wrong, get returns null." + bcolors.RESET)
        pass

    def do_multi_get(self, arg):
        '''
        multi_get <key> [subgroup_type= subgroup_index= shard_index=]
        =========
        Get an Object using multi_get

        key:            the key string

        *** optional key arguments ***
        subgroup_type:              optional subgroup type, could be either of the following:
                                    VolatileCascadeStoreWithStringKey
                                    PersistentCascadeStoreWithStringKey
                                    TriggerCascadeNoStoreWithStringKey
                                    by default, put try to find object pool by parsing key
        subgroup_index:             the subgroup index
        shard_index:                the shard index
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 1:
            print(bcolors.FAIL + 'At least one argument is required.' + bcolors.RESET)
        else:
            subgroup_type = ""
            subgroup_index = 0
            shard_index = 0
            argpos = 1
            while argpos < len(args):
                extra_option = args[argpos].split('=')
                if len(extra_option) != 2:
                    print(bcolors.FAIL + "Unknown argument:" + args[2] + bcolors.RESET)
                    return
                elif extra_option[0] == 'subgroup_type':
                    subgroup_type = extra_option[1]
                elif extra_option[0] == 'subgroup_index':
                    subgroup_index = int(extra_option[1],0)
                elif extra_option[0] == 'shard_index':
                    shard_index = int(extra_option[1],0)
                argpos = argpos + 1
            res = self.capi.multi_get(args[0],subgroup_type=subgroup_type,subgroup_index=subgroup_index,shard_index=shard_index)
            if res:
                odict = res.get_result()
                print(bcolors.OK + f"{str(odict)}" + bcolors.RESET)
            else:
                print(bcolors.FAIL + "Something went wrong, get returns null." + bcolors.RESET)
        pass

    def do_get_size(self, arg):
        '''
        get_size <key> [subgroup_type= subgroup_index= shard_index= stable= version= timestamp=]
        ========
        Get the size of an bject

        key:            the key string

        *** optional key arguments ***
        subgroup_type:              optional subgroup type, could be either of the following:
                                    VolatileCascadeStoreWithStringKey
                                    PersistentCascadeStoreWithStringKey
                                    TriggerCascadeNoStoreWithStringKey
                                    by default, put try to find object pool by parsing key
        subgroup_index:             the subgroup index
        shard_index:                the shard index
        stable:                     If this is a stable read or not, default to staVble.
        version:                    the version. For versioned get only
        timestamp:                  the unix EPOCH timestamp in microseconds. For timestamped-get only
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 1:
            print(bcolors.FAIL + 'At least one argument is required.' + bcolors.RESET)
        else:
            subgroup_type = ""
            subgroup_index = 0
            shard_index = 0
            stable = True
            version = ServiceClientAPI.CURRENT_VERSION
            timestamp = 0
            argpos = 1
            while argpos < len(args):
                extra_option = args[argpos].split('=')
                if len(extra_option) != 2:
                    print(bcolors.FAIL + "Unknown argument:" + args[2] + bcolors.RESET)
                    return
                elif extra_option[0] == 'subgroup_type':
                    subgroup_type = extra_option[1]
                elif extra_option[0] == 'subgroup_index':
                    subgroup_index = int(extra_option[1],0)
                elif extra_option[0] == 'shard_index':
                    shard_index = int(extra_option[1],0)
                elif extra_option[0] == 'stable':
                    if extra_option[1].lower == 'false' or extra_option[1].lower == 'no' or extra_option[1].lower() == 'off' or extra_option[1].lower() == '0':
                        stable = False;
                elif extra_option[0] == 'version':
                    version = int(extra_option[1],0)
                elif extra_option[0] == 'timestamp':
                    timestamp = int(extra_option[1],0)
                argpos = argpos + 1
            res = self.capi.get_size(args[0],subgroup_type=subgroup_type,subgroup_index=subgroup_index,shard_index=shard_index,version=version,timestamp=timestamp)
            if res:
                obj_size = res.get_result()
                print(bcolors.OK + f"Serialized object is {obj_size} bytes" + bcolors.RESET)
            else:
                print(bcolors.FAIL + "Something went wrong, get returns null." + bcolors.RESET)
        pass

    def do_multi_get_size(self, arg):
        '''
        multi_get_size <key> [subgroup_type= subgroup_index= shard_index=]
        ==============
        Get the size of an object using multi_get

        key:            the key string

        *** optional key arguments ***
        subgroup_type:              optional subgroup type, could be either of the following:
                                    VolatileCascadeStoreWithStringKey
                                    PersistentCascadeStoreWithStringKey
                                    TriggerCascadeNoStoreWithStringKey
                                    by default, put try to find object pool by parsing key
        subgroup_index:             the subgroup index
        shard_index:                the shard index
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 1:
            print(bcolors.FAIL + 'At least one argument is required.' + bcolors.RESET)
        else:
            subgroup_type = ""
            subgroup_index = 0
            shard_index = 0
            argpos = 1
            while argpos < len(args):
                extra_option = args[argpos].split('=')
                if len(extra_option) != 2:
                    print(bcolors.FAIL + "Unknown argument:" + args[2] + bcolors.RESET)
                    return
                elif extra_option[0] == 'subgroup_type':
                    subgroup_type = extra_option[1]
                elif extra_option[0] == 'subgroup_index':
                    subgroup_index = int(extra_option[1],0)
                elif extra_option[0] == 'shard_index':
                    shard_index = int(extra_option[1],0)
                argpos = argpos + 1
            res = self.capi.multi_get_size(args[0],subgroup_type=subgroup_type,subgroup_index=subgroup_index,shard_index=shard_index)
            if res:
                obj_size = res.get_result()
                print(bcolors.OK + f"Serialized object is {obj_size} bytes" + bcolors.RESET)
            else:
                print(bcolors.FAIL + "Something went wrong, get returns null." + bcolors.RESET)
        pass

    def do_list_keys_in_shard(self, arg):
        '''
        list_keys_in_shard <subgroup_type> [subgroup_index= shard_index= stable= version= timestamp=]
        ==================
        List the keys in a shard
        subgroup_type:      
                                    VolatileCascadeStoreWithStringKey
                                    PersistentCascadeStoreWithStringKey
                                    TriggerCascadeNoStoreWithStringKey

        *** optional key arguments ***
        subgroup_index:             the subgroup index, default to 0
        shard_index:                the shard index, default to 0
        stable:                     If this is a stable read or not, default to staVble.
        version:                    the version. For versioned get only
        timestamp:                  the unix EPOCH timestamp in microseconds. For timestamped-get only
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 1:
            print(bcolors.FAIL + 'At least one argument is required.' + bcolors.RESET)
        else:
            subgroup_index = 0
            shard_index = 0
            stable = True
            version = ServiceClientAPI.CURRENT_VERSION
            timestamp = 0
            argpos = 1
            while argpos < len(args):
                extra_option = args[argpos].split('=')
                if len(extra_option) != 2:
                    print(bcolors.FAIL + "Unknown argument:" + args[2] + bcolors.RESET)
                    return
                elif extra_option[0] == 'subgroup_index':
                    subgroup_index = int(extra_option[1],0)
                elif extra_option[0] == 'shard_index':
                    shard_index = int(extra_option[1],0)
                elif extra_option[0] == 'stable':
                    if extra_option[1].lower == 'false' or extra_option[1].lower == 'no' or extra_option[1].lower() == 'off' or extra_option[1].lower() == '0':
                        stable = False;
                elif extra_option[0] == 'version':
                    version = int(extra_option[1],0)
                elif extra_option[0] == 'timestamp':
                    timestamp = int(extra_option[1],0)
                argpos = argpos + 1
            res = self.capi.list_keys_in_shard(args[0],subgroup_index=subgroup_index,shard_index=shard_index,version=version,stable=stable,timestamp=timestamp)
            if res:
                keys = res.get_result()
                print(bcolors.OK + f" {str(keys)}" + bcolors.RESET)
            else:
                print(bcolors.FAIL + "Something went wrong, get returns null." + bcolors.RESET)
        pass

    def do_multi_list_keys_in_shard(self, arg):
        '''
        multi_list_keys_in_shard <subgroup_type> [subgroup_index= shard_index=]
        ========================
        List the keys in a shard
        subgroup_type:      
                                    VolatileCascadeStoreWithStringKey
                                    PersistentCascadeStoreWithStringKey
                                    TriggerCascadeNoStoreWithStringKey

        *** optional key arguments ***
        subgroup_index:             the subgroup index, default to 0
        shard_index:                the shard index, default to 0
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 1:
            print(bcolors.FAIL + 'At least one argument is required.' + bcolors.RESET)
        else:
            subgroup_index = 0
            shard_index = 0
            argpos = 1
            while argpos < len(args):
                extra_option = args[argpos].split('=')
                if len(extra_option) != 2:
                    print(bcolors.FAIL + "Unknown argument:" + args[2] + bcolors.RESET)
                    return
                elif extra_option[0] == 'subgroup_index':
                    subgroup_index = int(extra_option[1],0)
                elif extra_option[0] == 'shard_index':
                    shard_index = int(extra_option[1],0)
                argpos = argpos + 1
            res = self.capi.multi_list_keys_in_shard(args[0],subgroup_index=subgroup_index,shard_index=shard_index)
            if res:
                keys = res.get_result()
                print(bcolors.OK + f" {str(keys)}" + bcolors.RESET)
            else:
                print(bcolors.FAIL + "Something went wrong, get returns null." + bcolors.RESET)
        pass

    def do_list_keys_in_object_pool(self, arg):
        '''
        list_keys_in_object_pool <object_pool_pathname> [stable= version= timestamp=]
        ========================
        List the keys in an object pool.
        object_pool_pathname:       The object pool

        *** optional key arguments ***
        stable:                     If this is a stable read or not, default to staVble.
        version:                    the version. For versioned get only
        timestamp:                  the unix EPOCH timestamp in microseconds. For timestamped-get only
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 1:
            print(bcolors.FAIL + 'At least one argument is required.' + bcolors.RESET)
        else:
            stable = True
            timestamp = 0
            version = ServiceClientAPI.CURRENT_VERSION
            argpos = 1
            while argpos < len(args):
                extra_option = args[argpos].split('=')
                if len(extra_option) != 2:
                    print(bcolors.FAIL + "Unknown argument:" + args[2] + bcolors.RESET)
                    return
                elif extra_option[0] == 'stable':
                    if extra_option[1].lower == 'false' or extra_option[1].lower == 'no' or extra_option[1].lower() == 'off' or extra_option[1].lower() == '0':
                        stable = False;
                elif extra_option[0] == 'version':
                    version = int(extra_option[1],0)
                elif extra_option[0] == 'timestamp':
                    timestamp = int(extra_option[1],0)
                argpos = argpos + 1
            results = self.capi.list_keys_in_object_pool(args[0],stable=stable,version=version,timestamp=timestamp)
            if results:
                print(bcolors.OK + '[' + bcolors.RESET)
                for res in results:
                    keys = res.get_result()
                    print(bcolors.OK + f" {str(keys)}" + bcolors.RESET)
                print(bcolors.OK + ']' + bcolors.RESET)
            else:
                print(bcolors.FAIL + "Something went wrong, get returns null." + bcolors.RESET)
        pass

    def do_multi_list_keys_in_object_pool(self, arg):
        '''
        multi_list_keys_in_object_pool <object_pool_pathname>
        ==============================
        List the keys in an object pool.
        object_pool_pathname:       The object pool
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 1:
            print(bcolors.FAIL + 'At least one argument is required.' + bcolors.RESET)
        else:
            results = self.capi.multi_list_keys_in_object_pool(args[0])
            if results:
                print(bcolors.OK + '[' + bcolors.RESET)
                for res in results:
                    keys = res.get_result()
                    print(bcolors.OK + f" {str(keys)}" + bcolors.RESET)
                print(bcolors.OK + ']' + bcolors.RESET)
            else:
                print(bcolors.FAIL + "Something went wrong, get returns null." + bcolors.RESET)
        pass

    def do_create_object_pool(self, arg):
        '''
        create_object_pool <pathname> <subgroup_type> <subgroup_index>
        ==================
        Create an object pool

        pathname:       the absolute pathname for the object pool
        subgroup_type:  the type of the subgroup for this object pool, could be either of the following:
                        VolatileCascadeStoreWithStringKey
                        PersistentCascadeStoreWithStringKey
                        TriggerCascadeNoStoreWithStringKey
        subgroup_index: the subgroup index

        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 3:
            print(bcolors.FAIL + 'At least three arguments are required.' + bcolors.RESET)
        else:
            subgroup_index = int(args[2],0)
            res = self.capi.create_object_pool(args[0],args[1],subgroup_index)
            if res:
                ver = res.get_result()
                print(bcolors.OK + f"{ver}" + bcolors.RESET)
            else:
                print(bcolors.FAIL + "Something went wrong, create_object_pool returns null." + bcolors.RESET)

    def do_list_object_pools(self, arg):
        '''
        list_object_pools
        =================
        list all object pools
        '''
        self.check_capi()
        args = arg.split()
        res = self.capi.list_object_pools()
        print(bcolors.OK + f"{res}" + bcolors.RESET)

    def do_get_object_pool(self, arg):
        '''
        get_object_pool <pathname>
        ===============
        get object pool details
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 1:
            print(bcolors.FAIL + 'At least one argument is required.' + bcolors.RESET)
        else:
            res = self.capi.get_object_pool(args[0])
            print(bcolors.OK + f"{res}" + bcolors.RESET)

    def do_timestamp_logger(self, arg):
        '''
        timestamp_logger <command> ...
        ================
        Test timestamp_logger function

        command:        could be either 'log', 'clear', and 'flush'.

        case 'log':
        timestamp_logger log <tag> <msg_id> <extra>
        , where
        tag:            the event tag, as listed in include/cascade/utils.hpp
        msg_id:         an integer for message id, given by the user.
        extra:          an extra integer as a memo space, its value is up to the user. It will appear as the last
                        components of the timestamp log.

        case 'clear':
        timestamp_logger clear

        case 'flush':
        timestamp_logger flush <filename>
        , where
        filename:       the log filename.
        '''
        self.check_capi()
        args = arg.split()
        if len(args) < 1:
            print(bcolors.FAIL + 'At least one argument is required.' + bcolors.RESET)
            return;
        cmd = args[0]
        tl = TimestampLogger()
        if cmd == 'log':
            if len(args) < 4:
                print(bcolors.FAIL + 'Please provide tag, message id, AND extra arguments.' + bcolors.RESET)
                return
            tag = int(args[1])
            msg_id = int(args[2])
            extra = int(args[3])
            tl.log(int(tag),self.capi.get_my_id(),msg_id,extra)
        elif cmd == 'clear':
            tl.clear()
        elif cmd == 'flush':
            if len(args) < 2:
                print(bcolors.FAIL + 'No filename is given.' + bcolors.RESET)
                return
            tl.flush(args[1],True)
        else:
            print(bcolors.FAIL + f"Unknown timestamp_logger command: {cmd}." + bcolors.RESET)

    # end of CascadeClientShell definition

if __name__ == '__main__':
    CascadeClientShell().cmdloop()
