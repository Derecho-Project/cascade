#!/usr/bin/env python3
import abc
import numpy as np

class UserDefinedLogic(abc.ABC):
    '''
    The base class for all Python UDLs. This class is embedded in cascade service and can only load from there.
    '''

    @abc.abstractmethod
    def __init__(self,conf):
        '''
        The constructor of the python udl
        
        positional argument(s):
        conf                -- (string) the configuration from application configuration ( the
                               "user_defined_logic_config_list" option in dfgs.json)
        '''
        pass

    @abc.abstractmethod
    def ocdpo_handler(self,**kwargs):
        '''
        The entry point of the user defined python logic. This function should 

        positional argument(s):

        keyword argument(s):
        sender              -- (long) the sender id
        pathname            -- (string) the object pool pathname
        key                 -- (string) the key of this object
        blob                -- (1-dim numpy array of type int8) the data
        worker_id           -- (long) the off-critical data path worker id
        '''
        pass

    @abc.abstractmethod
    def __del__(self):
        '''
        The destructor

        positional argument(s):
        context             -- the cascade context TODO: to be implemented
        '''
        pass
