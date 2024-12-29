from fyers_apiv3.FyersWebsocket import data_ws
import time 
import datetime as dt
import Save
import json
import os
import dotenv
import redis
import random


class _Data():
    def __init__(self,redis_client:redis.Redis,access_token:str,datatype=None,testing=bool):
        self.access_token = access_token
        dotenv.load_dotenv()
        self.testing = testing
        self.stonks = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"] # list of stonks in "NSE:SBIN-EQ" this format
        self._connected = False
        self._subscribed = False
        self._litemode = False
        self.data_type = datatype # defined in subclasses
        self.keys = json.loads(os.getenv("DATA_FIELDS"))[datatype]
        self.r = redis_client
        #datetime in YYYY-MM-DD format
        self.india_date = dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")
        
        # initialising the save files. 
       


    """ the following 5 functions are for the websocket to use and implement"""

    def onmessage(self,message):
        print("Response:", message)
        if 'symbol' in message.keys():
            self.r.xadd(message['symbol'].split('-')[0],message)


    def onerror(self,message):
        print("Error:", message)
        if message == 'Connection to remote host was lost.':
            self._subscribed = False
            print('canceled subscription')
       
    def onclose(self,message):
        print("Connection closed:", message)

    def onopen(self):
        print('connection opened')
        self._connected = True
        if not self._subscribed: # indicates some error happened
            self.subscribe()
            print('reestablished connection')
            self._subscribed = True

    def connect(self):
        self.fyers  = data_ws.FyersDataSocket(
        access_token=self.access_token,       # Access token in the format "appid:accesstoken"
        log_path='',                     # Path to save logs. Leave empty to auto-create logs in the current directory.
        litemode=self._litemode,                  # Lite mode disabled. Set to True if you want a lite response.
        write_to_file=False,              # Save response in a log file instead of printing it.
        reconnect=True,                  # Enable auto-reconnection to WebSocket on disconnection.
        on_connect=self.onopen,               # Callback function to subscribe to data upon connection.
        on_close=self.onclose,                # Callback function to handle WebSocket connection close events.
        on_error=self.onerror,                # Callback function to handle WebSocket errors.
        on_message=self.onmessage,            # Callback function to handle incoming messages from the WebSocket.
        reconnect_retry=10               # Number of times reconnection will be attepmted in case
        )
        self.fyers.connect()
        self.r.set('end','false')
        self._connected = True
        

    
    def subscribe(self):
        """
        subscribes to websocket to begin recieving a stream of ticker data.
        """
        #data type: DepthUpdate, SymbolUpdate
        if self._connected:
            self.fyers.subscribe(symbols=self.stonks,data_type="SymbolUpdate" if self.data_type=="symbol" else "DepthUpdate") # subscribe to websocket
            self.fyers.keep_running()
            self._subscribed = True
        else:
            print(f'initialise websocket via .connect()')

    def unsubscribe(self):
        """
        unsubscribes to websocket to halt recieving a stream of ticker data.
        """
        if self._subscribed:
            self.fyers.unsubscribe(symbols=self.stonks,data_type="SymbolUpdate" if self.data_type=="symbol" else "DepthUpdate") #unsubscribe to websocket
            
            print(f"unsubscribed from {self.data_type}")
            self._subscribed=False # not having this seems to cause some bugs (i.e it wont unsubscribe)
    
    def close(self):
        """
        closes the websocket connection.
        """
        if self._connected:
            self.fyers.close_connection()
            self.r.set('end','true')
            self._connected = False
        else:
            raise Exception("connect to the server first!")
    def keepAlive(self):
        dotenv.load_dotenv()
        stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if self.testing else json.loads(os.getenv("STOCKS"))["REAL"]
        while self.r.get('end') !=b'true':
            check = random.randint(0,len(stonksList)-1)
            if self.r.xread({stonksList[check].split('-')[0]:'$'},block=5000) == []: # assuming shit's hit the fan
                for _ in range(10): # 10 tries to get it to do it's shit properly
                    self.fyers.close_connection()
                    self.fyers.connect()
                    self.subscribe()
                    check = random.randint(0,len(stonksList)-1)
                    time.sleep(3)
                    if self.r.xread({stonksList[check].split('-')[0]:'$'},block=5000) != []: # connection opened again.
                        break # we're good to go
                else:
                    self.r.set('end','true') # Fuck it we're done if fate doesn't want us to trade today
                    # *** LOG IT **
                    with open('log.txt','a') as f:
                        print(f'connection failed to uphold, time:{dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d:: %H:%M:%S")}, tough luck buddy')
            time.sleep(60)
class Symbol(_Data):
    def __init__(self,redis_client,access_token:str,testing:bool):
        super().__init__(redis_client,access_token,"symbol",testing)

class Depth(_Data):
    def __init__(self,redis_client,access_token:str,testing:bool):
        super().__init__(redis_client,access_token,"depth",testing)



class Connect():
    def __init__(self,access_token:str,testing):
        self.symb = Symbol(access_token,testing)
        self.depth = Symbol(access_token,testing)
        self._connnected = False

    def connect(self):
        self.symb.connect()
        self.depth.connect()
        self._connnected = True
    
    def subscribe(self):
        self.symb.subscribe()
        self.depth.subscribe()

    def disconnect(self):
        if self._connnected:
            self.symb.unsubscribe()
            self.depth.unsubscribe()
            self._connnected = False
        else:
            print(Exception("connect to the websocket first!"))

