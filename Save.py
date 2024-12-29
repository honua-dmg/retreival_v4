# done and dusted :)
import datetime as dt
import os
import h5py as h
import numpy as np
import pandas as pd
#import functions as func
import redis
import json
import dotenv


class Format():
    #Interface 
    def __init__(self,directory:str,testing:bool) -> None:
        
        self.dir = directory # to know where we have to save our shit
        self.initialised = False

        dotenv.load_dotenv()
        self.stonks = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
        self.india_date=dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")


    def initialise(self):
        self.save_loc=True
        #abstract 
        # creates files if not already made 
        pass

    def open_file(self):
        pass

    def close_file(self):
        pass

    def save_files(self,message):
        #abstract
        # a method to save incoming data to our files
        pass


class csv(Format):

    def __init__(self,directory,testing:bool) -> None:
        super().__init__(directory,testing)


    def _initcols(self,file_path,type):
        """
        args:
            file_path: location of csv file
        initialises columns within newly made csv files 
        
        """
        keys = json.loads(os.getenv("DATA_FIELDS"))['symbol'] if type=='symbol' else json.loads(os.getenv("DATA_FIELDS"))['depth']
        with open(file_path,'a+') as f:
            if os.path.getsize(file_path) != 0:  #if file is already made, no need to initialse it again
                return
           #creates columns in csv files
            for key in keys:
                f.write(key+',')
            f.write('time')
            f.write('\n')


    # *********************************************************************************************

    def initialise(self):
        """
        create individual csv files for each stock for said day
        """
        super().initialise()
        for stonk in self.stonks:
            #check if directories exist
            market,symbol = stonk.split('-')[0].split(':')
            directory = os.path.join(self.dir,market,symbol)

            if not os.path.exists(directory): #checking to see if file path exists
                try:                                            # we had to include this try block again due to issues in multiprocessing
                    os.makedirs(directory)           #we will make the file path if it doesnt :)
                except Exception:                   
                    print('file already exists')
            #check if file with type and datestamp is initialised
            # each file will have a symbol and depth file
            file_path_symb = os.path.join(directory,f'symbol-{self.india_date}.csv')
            file_path_dept = os.path.join(directory,f'depth-{self.india_date}.csv')
            self._initcols(file_path_symb,'symbol')
            self._initcols(file_path_dept,'depth')


    def save_msg(self,message):
        """
        args:
            message: data received from fyers
        
        parses message data and saves it to the right csv file
        """
        if self.save_loc==False:
            self.initialise()
            self.save_loc = True

        india_epoch = (dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5)).timestamp()
        if 'symbol' not in message.keys(): # a message without data
            return
       
    
        market,symbol = message['symbol'].split('-')[0].split(':')
        directory = os.path.join(self.dir,market,symbol)
        data_type = "symbol" if message['type'] =='sf' else "depth"

        file_path = os.path.join(directory,f'{data_type}-{self.india_date}.csv')
        #file_path = f'{self.dir}/{file_symbol}/{self.data_type[:4]}-{self.india_date}.csv'
        # appending data to our file as a line
        with open(file_path,'a+') as f:
            for key in message:
                f.write(str(message[key])+',')
            f.write(str(india_epoch))
            f.write('\n') # to go to a new line to save the next record 


