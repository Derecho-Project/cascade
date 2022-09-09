#!/usr/bin/env python3
from derecho.cascade.client import ServiceClientAPI

class bcolors:
    OK = '\033[92m' #GREEN
    WARNING = '\033[93m' #YELLOW
    FAIL = '\033[91m' #RED
    RESET = '\033[0m' #RESET COLOR

if __name__ == '__main__':
    print("KVS Client Example in Python.")

    print("1) Load configuration and connecting to cascade service...")
    capi = ServiceClientAPI()
    print("- connected.")

    print("2) Create a folder, a.k.a object pool in the first VolatileCascadeStore subgroup.")
    res = capi.create_object_pool("/vcss_objects","VolatileCascadeStoreWithStringKey",0)
    if res:
        ver = res.get_result()
        print(bcolors.OK + f"folder '/vcss_objects' is created with version:{ver}" + bcolors.RESET)
    else:
        print(bcolors.FAIL + "Something went wrong, create_object_pool returns null." + bcolors.RESET)
        quit()

    print("3) List all folders a.k.a. object pools:")
    res = capi.list_object_pools()
    print(bcolors.OK + f"{res}" + bcolors.RESET)

    print("4) Put an object with key '/vcss_objects/obj_001'")
    res = capi.put('/vcss_objects/obj_001',bytes('value of /vcss_objects/obj_001','utf-8'),previous_version=ServiceClientAPI.CURRENT_VERSION,previous_version_by_key=ServiceClientAPI.CURRENT_VERSION)
    if res:
        ver = res.get_result()
        print(bcolors.OK + f"Put is successful with version {ver}." + bcolors.RESET)
    else:
        print(bcolors.FAIL + "Something went wrong, put returns null." + bcolors.RESET)
        quit()

    print("5) Get an object with key '/vcss_objects/obj_001'")
    res = capi.get('/vcss_objects/obj_001')
    if res:
        odict = res.get_result()
        print(bcolors.OK + f"Get is successful with details: {odict}" + bcolors.RESET)
    else:
        print(bcolors.FAIL + "Something went wrong, get returns null." + bcolors.RESET)
        quit()
