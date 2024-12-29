
import Main
import pandas as pd
import redis
import time
class SingleavgParser():
    
    def __init__(self,symb_file_path,depth_file_path,file_name):
        self.r  = redis.Redis(host="localhost",port="6379",db=0)
        self.file_name = file_name
        self.symbFile = symb_file_path
        self.depthFile = depth_file_path
        if self.symbFile.split('.')[1] =='csv':
            self.symb = pd.read_csv(self.symbFile)
            self.depth = pd.read_csv(self.depthFile)
        else:
            self.symb = pd.read_excel(self.symbFile)
            self.depth = pd.read_excel(self.depthFile)
        self.df = pd.concat([self.symb,self.depth],ignore_index=True).sort_values(by='time')

    def emulateDataStream(self):
        self.r.set('end','false')
        for row in  self.df.rolling(1):
            message = {key: list(value.values())[0] for key,value in row.dropna(axis=1).to_dict().items()}
            self.r.xadd(message['symbol'].split('-')[0],message) 
            time.sleep(.05)
            
        self.r.set('end','true')

    def SignalParse(self):
        pass