# Final code:
import multiprocessing.process
import authToken
import wbsoc
import time
import redis
import Save
import json
import os
import dotenv
import avgParser
import multiprocessing
import threading
import logging
import pandas as pd
import datetime as dt
import avgSignaler as a 
import random
"""
    Considerations for speed:
    1. in avgParser, to get the signals, we read from the csv files we make to get past data- if this proves to be too slow for you
        put the data in redis and extract it from there. 
"""

def producer(testing,access_token=None,client=0):
    if access_token==None:
        token = authToken.AutoLogin(client).get_access_token()
    else:
        token = access_token
    r = redis.Redis(host="localhost",port="6379",db=0)
    symb = wbsoc.Symbol(r,token,testing)
    depth = wbsoc.Depth(r,token,testing)
    """
    alivesymb = threading.Thread(target = symb.keepAlive)
    alivedep = threading.Thread(target = depth.keepAlive)
    alivesymb.daemon = True
    alivesymb.daemon = True
    alivedep.start()
    alivesymb.start()
    """
    symb.connect()
    depth.connect()
    symb.subscribe()
    depth.subscribe()
    time.sleep(60*60*6)
    print("its time to end!")
    depth.unsubscribe()
    symb.unsubscribe()
    symb.close()
    depth.close()

## hhee


# saves all incoming data to csv files :)
def csvWorker(directory,testing): # should be on a separate process
    dotenv.load_dotenv()
    r = redis.Redis(host="localhost",port="6379",db=0)
    stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
    worker = Save.csv(directory,testing)
    stonks = {stonk.split('-')[0]:'$' for stonk in stonksList}
    worker.initialise()

    while r.get('end')!=b'true': # continuosly reading the incoming stream of data.
        messages = r.xread(stonks,block=100)
        if messages == []:
            continue
        print(messages)
        for stream in messages:
            for uncoded_msg in stream[1]:
                try:
                    worker.save_msg({key.decode('utf-8'): value.decode('utf-8') for key, value in uncoded_msg[1].items()})
                except Exception as e:
                    print(e)
                    logging.log(msg=f"{e} this went wrong :)")
                    return



def avgParserWorker(directory,testing):
    dotenv.load_dotenv()
    r = redis.Redis(host="localhost",port="6379",db=0)
    t = redis.Redis(host="localhost",port="6379",db=1)

    
    stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
    stonks = {stonk.split('-')[0]:'$' for stonk in stonksList}

    while r.get('end')!=b'true': # continuosly reading the incoming stream of data.
        data = r.xread(stonks,block=100)
        for stream in data:
            for uncoded_msg in stream[1]:
                msg = {key.decode('utf-8'): value.decode('utf-8') for key, value in uncoded_msg[1].items()} # decoding message
                
                parsed_msg = avgParser.parseMsg(t,msg)
                #print(parsed_msg)
                # saving the file to csv
                avgParser.to_csv(parsed_msg,directory)
   
    t.flushdb()

def SignalWorker(testing):
    polarisers={}
    dotenv.load_dotenv()
    r = redis.Redis(host="localhost",port="6379",db=0)

    avg_r = redis.Redis(host="localhost",port="6379",db=2)
    stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
    stonks = {stonk.split('-')[0]:'$' for stonk in stonksList}

    while r.get('end')!=b'true': # continuosly reading the incoming stream of data.
        data = r.xread(stonks,block=100)
        for stream in data:
            for uncoded_msg in stream[1]:
                msg = {key.decode('utf-8'): value.decode('utf-8') for key, value in uncoded_msg[1].items()} # decoding message
                parsed_msg = avgParser.parseMsg(avg_r,msg)
                print(parsed_msg)
                if parsed_msg ==None:
                    continue
                #looking for signals
                try:
                    print('entering signal finder')
                    a.SignalFinder(parsed_msg,avg_r,polarisers[parsed_msg['stonk']])
                except Exception: # incase we haven't made it yet
                    print('faq, making polariser element')
                    polarisers[parsed_msg['stonk']] = {}
                    a.SignalFinder(parsed_msg,avg_r,polarisers[parsed_msg['stonk']])
    pd.DataFrame(polarisers).to_csv('polariser.csv')


    
def endDay(testing): # clears cache. 
    dotenv.load_dotenv()
    stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
    r = redis.Redis(host="localhost",port="6379",db=0)
    r.flushall()



if __name__ =="__main__":
    testing = False
    producerProcess =multiprocessing.Process(producer,args=(testing))

    csvWorkerProcess = multiprocessing.Process(csvWorker,args=('./data',testing))
    avgParserWorkerProcess = multiprocessing.Process(avgParserWorker,args=('./averages',testing))
    
    producerProcess.start()
    csvWorkerProcess.start()
    avgParserWorkerProcess.start()


    producerProcess.join()
    csvWorkerProcess.join()
    avgParserWorkerProcess.join()

    endDay(testing)



def threadripper(token=None,testing=True):
    if token==None:
        producerProcess =threading.Thread(target=producer,args=(testing))
    else:
        producerProcess =threading.Thread(target=producer,args=(testing,token))
    csvWorkerProcess = threading.Thread(target=csvWorker,args=('./data',testing))
    avgParserWorkerProcess = threading.Thread(target=avgParserWorker,args=('./averages',testing))
    
    producerProcess.start()
    csvWorkerProcess.start()
    avgParserWorkerProcess.start()


    producerProcess.join()
    csvWorkerProcess.join()
    avgParserWorkerProcess.join()

    endDay(testing)

def processripper(token=None,testing=False):
    if token==None:
        producerProcess =multiprocessing.Process(target=producer,args=(testing))
    else:
        producerProcess =multiprocessing.Process(target=producer,args=(testing,token))
    csvWorkerProcess = multiprocessing.Process(target=csvWorker,args=('./data',testing))
    avgParserWorkerProcess = multiprocessing.Process(target=avgParserWorker,args=('./averages',testing))
    
    producerProcess.start()
    csvWorkerProcess.start()
    avgParserWorkerProcess.start()


    producerProcess.join()
    csvWorkerProcess.join()
    avgParserWorkerProcess.join()

    endDay(testing)