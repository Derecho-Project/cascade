#!/usr/bin/env python3
from derecho.cascade.udl import UserDefinedLogic
import cascade_context
import numpy as np
import json
import re
from derecho.cascade.member_client import ServiceClientAPI

class ConsolePrinterUDL(UserDefinedLogic):
    '''
    ConsolePrinter is the simplest example showing how to use the udl
    '''
    def __init__(self,conf_str):
        '''
        Constructor
        '''
        super(ConsolePrinterUDL,self).__init__(conf_str)
        self.conf = json.loads(conf_str)
        print(f"ConsolePrinter constructor received json configuration: {self.conf}")
        pass

    def ocdpo_handler(self,**kwargs):
        '''
        The off-critical data path handler
        '''
        print(f"I recieved kwargs: {kwargs}")

    def __del__(self):
        '''
        Destructor
        '''
        print(f"ConsolePrinterUDL destructor")
        pass

class WordCountMapper(UserDefinedLogic):
    '''
    Word Count Mapper
    '''
    def __init__(self,conf_str):
        '''
        Constructor
        '''
        super(WordCountMapper,self).__init__(conf_str)
        self.conf = json.loads(conf_str)
        print('WordCountMapper.__init__() is called.')
        self.capi = ServiceClientAPI()
        pass

    def ocdpo_handler(self,**kwargs):
        '''
        The off-critical data path handler
        '''
        print(f"I recieved kwargs: {kwargs}")
        sender = kwargs["sender"]
        key = kwargs["key"]
        pathname = kwargs["pathname"]
        blob = kwargs["blob"]
        worker_id = kwargs["worker_id"]
        text = blob.tobytes().decode()
        word_count = {}
        # split words
        for word in re.split('\W',text):
            if word in word_count:
                word_count[word] += 1
            else:
                word_count[word] = 1
        # emit key:count
        # Please note that count is in a 1-element numpy array of type int32
        print(f"Just before emit mapper output:{word_count}")
        for word in word_count:
            cascade_context.emit(word,np.array([word_count[word]],dtype=np.int32))
        print("emit done.")
        print(f"Nodes in Cascade service : {str(self.capi.get_members())}")

    def __del__(self):
        '''
        Destructor
        '''
        print(f"WordCountMapper destructor")
        pass

class WordCountReducer(UserDefinedLogic):
    '''
    Word Count Reducer
    '''
    def __init__(self,conf_str):
        '''
        Constructor
        '''
        super(WordCountReducer,self).__init__(conf_str)
        self.conf = json.loads(conf_str)
        self.word_count = {}
        print('WordCountReducer.__init__() is called.')
        pass

    def ocdpo_handler(self,**kwargs):
        '''
        The off-critial data path handler
        '''
        sender = kwargs["sender"]
        key = kwargs["key"]
        pathname = kwargs["pathname"]
        blob = kwargs["blob"]
        worker_id = kwargs["worker_id"]
        # merge the word count
        if key in self.word_count:
            self.word_count[key] += blob.view(dtype=np.int32)[0]
        else:
            self.word_count[key] = blob.view(dtype=np.int32)[0]
        print(self.word_count)

    def __del__(self):
        '''
        Destructor
        '''
        print(f"WordCountReducer destructor")
        pass
