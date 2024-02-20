# Python API
Cascade service supports data access with Python. If you didn't see `<build_path>/src/service/python/cascade_client.py`, it's probably because the python support is not enabled. Please following the [build  guidance](../../../README.md#build-cascade) for how to enable python support. Otherwise, python support is enabled and you can just use `cascade_client.py` as `cascade_client` to test the python support. It also relies on the `derecho.cfg` configuration file in current work directory, as the C++ client did.
 
 Cascade python API is managed in `derecho.cascade.client.ServiceClientAPI` class. To use this API, you just import the `derecho.cascade.client` package and create an object of type `ServiceClientAPI` as follows.
 ```
 from derecho.cascade.client import ServiceClientAPI
 capi = ServiceClientAPI()
 ```
 The constructor of ServiceClientAPI will load the cascade configurations in `derecho.cfg` and try connecting to the cascade service. Once it successfully connects, you can just use the methods defined in `ServiceClientAPI` class, which mirror the C++ [`ServiceClientAPI` interface](../../../include/cascade/service.hpp#L188). Please use [`cascade_client.py`](cascade_client.py) as example. We also provide a simpler example [kvs_client.py](../../applications/standalone/kvs_client/kvs_client.py) for quickstart.
 
 To use this api in your python application, please install cascade following the main [installation guide](../../../README.md#installation). The python client support should be installed using `pip` along with `make install`.
 
 Then you can start python command and import cascade to verify installation as follows:
 ```
# python
Python 3.8.10 (default, Nov 26 2021, 20:14:08) 
[GCC 9.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> from derecho.cascade.external_client import ServiceClientAPI
>>> dir(ServiceClientAPI)
['CASCADE_SUBGROUP_TYPES', 'CURRENT_VERSION', '__class__', '__delattr__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', 'create_object_pool', 'get', 'get_member_selection_policy', 'get_members', 'get_object_pool', 'get_shard_members', 'list_object_pools', 'put', 'remove', 'set_member_selection_policy']
```
To see the document of the APIs, you can just print their `__doc__` attribute. For example, to show the document for `put` and `get`
```
>>> print(ServiceClientAPI.put.__doc__)
put(self: derecho.cascade.client.ServiceClientAPI, arg0: str, arg1: bytes, **kwargs) -> object

Put an object. 
The new object would replace the old object with the same key.
        @arg0    key 
        @arg1    value 
        ** Optional keyword argument: ** 
        @argX    subgroup_type   VolatileCascadeStoreWithStringKey | 
                                 PersistentCascadeStoreWithStringKey | 
                                 TriggerCascadeNoStoreWithStringKey 
        @argX    subgroup_index  
        @argX    shard_index     
        @argX    pervious_version        
        @argX    pervious_version_by_key 
        @argX    blocking 
        @argX    trigger         Using trigger put, always non-blocking regardless of blocking argument.
        @argX    message_id 
        @return  a future of the (version,timestamp) for blocking put; or 'False' object for non-blocking put.

>>> print(ServiceClientAPI.get.__doc__)
get(self: derecho.cascade.client.ServiceClientAPI, arg0: str, **kwargs) -> object

Get an an object. 
        @arg0    key 
        ** Optional keyword argument: ** 
        @argX    subgroup_type   VolatileCascadeStoreWithStringKey | 
                                 PersistentCascadeStoreWithStringKey | 
                                 TriggerCascadeNoStoreWithStringKey 
        @argX    subgroup_index  
        @argX    shard_index     
        @argX    version         Specify version for a versioned get.
        @argX    timestamp       Specify timestamp (as an integer in unix epoch microsecond) for a timestampped get.
        @return  a dict version of the object.

>>> 
```
