
import Main
import pandas as pd
import redis
import time
import avgParser
import avgSignaler as a
import threading

import json
import os
import matplotlib.pyplot as plt
from matplotlib.widgets import Button
import numpy as np
from matplotlib.ticker import MultipleLocator


class SingleavgParser():
    
    def __init__(self,symb_file_path,depth_file_path,file_name):
        self.r  = redis.Redis(host="localhost",port="6379",db=4)
        self.avg_r = redis.Redis(host="localhost",port="6379",db=5)
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
            #print(message)
            time.sleep(.05)
        self.r.set('end','true')

    def signals(self,graph=True):
 
        polarisers={}
        
        self.avg_r = redis.Redis(host="localhost",port="6379",db=2)


        stonks = {self.file_name:'$'}

        while self.r.get('end')!=b'true': # continuosly reading the incoming stream of data.
            data = self.r.xread(stonks,block=100)
            for stream in data:
                for uncoded_msg in stream[1]:
                    msg = {key.decode('utf-8'): value.decode('utf-8') for key, value in uncoded_msg[1].items()} # decoding message
                    parsed_msg = avgParser.parseMsg(self.avg_r,msg)
                    
                    if parsed_msg ==None:
                        continue
                    #looking for signals
                    #print(parsed_msg)
                    try:
                        #print('entering signal finder')
                        a.SignalFinder(parsed_msg,self.avg_r,polarisers[parsed_msg['stonk']])
                    except Exception: # incase we haven't made it yet
                        #print('faq, making polariser element')
                        polarisers[parsed_msg['stonk']] = {}
                        #print('made polariser dict')
                        a.SignalFinder(parsed_msg,self.avg_r,polarisers[parsed_msg['stonk']])
        #pd.DataFrame(polarisers).to_csv('polariser.csv')

    def graph(self):
        plt.ion()


        self.avg_r = redis.Redis(host="localhost", port="6379", db=2)
        


        stonksList = [self.file_name]

        # Data structures for graph navigation, initially store empty data
        graph_data = {stock: {"red": [], "green": [], "shorts": [], "buys": []} for stock in stonksList}
        current_index = 0  # Default to the first stock

        fig, ax = plt.subplots()
        plt.subplots_adjust(bottom=0.2)

        # Initialize the plot for red and green lines
        red_line, = ax.plot([], [], color="#B22222", linewidth=1, label="Red")
        green_line, = ax.plot([], [], color="#228B22", linewidth=1, label="Green")

        def update_plot():
            """Update the graph for the selected stock."""
            nonlocal current_index
            stock_name = stonksList[current_index]

            # Get data for the current stock
            red = graph_data[stock_name]["red"]
            green = graph_data[stock_name]["green"]
            shorts = graph_data[stock_name]["shorts"]
            buys = graph_data[stock_name]["buys"]

            ax.cla()  # Clear the axes but keep labels, title, etc.

            # Plot the red and green lines
            red_line.set_data(range(len(red)), red)
            green_line.set_data(range(len(green)), green)
            ax.plot(red_line.get_xdata(), red_line.get_ydata(), color="#B22222", linewidth=1, label="Red")
            ax.plot(green_line.get_xdata(), green_line.get_ydata(), color="#228B22", linewidth=1, label="Green")

            # Plot shorts and buys
            for short in shorts:
                ax.plot(short[0], short[1], "ro", markersize=5)
            for buy in buys:
                ax.plot(buy[0], buy[1], "bo", markersize=5)

            # Set plot labels and title
            ax.set_title(f"Graph for {stock_name}")
            ax.xaxis.set_major_locator(MultipleLocator(100))
            ax.yaxis.set_major_locator(MultipleLocator(10))
            ax.legend()
            fig.canvas.draw()

        def next_graph(event):
            nonlocal current_index
            current_index = (current_index + 1) % len(stonksList)
            update_plot()

        def prev_graph(event):
            nonlocal current_index
            current_index = (current_index - 1) % len(stonksList)
            update_plot()

        axprev = plt.axes([0.1, 0.05, 0.1, 0.075])  # Position: left
        axnext = plt.axes([0.8, 0.05, 0.1, 0.075])  # Position: right
        bnext = Button(axnext, "Next")
        bprev = Button(axprev, "Previous")
        bnext.on_clicked(next_graph)
        bprev.on_clicked(prev_graph)

        def update_data():
            """Fetch and update data for the current graph (only the active stock)."""
            # Only fetch data for the current stock
            stock_name = stonksList[current_index]
            data = self.avg_r.xread({stock_name + "GRAPH": "$"}, block=100)
            print("checking for updates")
            if not data:
                print('no data')
                return

            for stream in data:
                msg = {key.decode("utf-8"): value.decode("utf-8") for key, value in stream[1][0][1].items()}
                print(msg)
                red = [int(float(x)) if x != "null" else np.nan for x in msg["red"].split(",")]
                green = [int(float(x)) if x != "null" else np.nan for x in msg["green"].split(",")]

                shorts = [
                    (int(float(point["count"])), int(float(point["ltp"])))
                    for point in (
                        {k.decode("utf-8"): v.decode("utf-8") for k, v in msg.items()}
                        for _, msg in self.avg_r.xrange(stock_name + "-short")
                    )
                ]
                buys = [
                    (int(float(point["count"])), int(float(point["ltp"])))
                    for point in (
                        {k.decode("utf-8"): v.decode("utf-8") for k, v in msg.items()}
                        for _, msg in self.avg_r.xrange(stock_name + "-long")
                    )
                ]

                # Update the current stock's data
                print({"red": red, "green": green, "shorts": shorts, "buys": buys})
                graph_data[stock_name] = {"red": red, "green": green, "shorts": shorts, "buys": buys}

        # Enter data update loop
        while self.r.get("end") != b"true":
            update_data()  # Fetch and update data only for the current stock
            update_plot()  # Update the plot with the current stock data
            plt.pause(0.005)  # Allow for real-time interaction

        plt.show()

    def reset(self):
        self.r.flushall()

    def start(self):
        emulator = threading.Thread(target = self.emulateDataStream)
        signaler = threading.Thread(target = self.signals)
        self.graph()
        signaler.start()
        
        emulator.start()
        
        signaler.join()
        emulator.join()


if __name__ == "__main__":
    symb_file='data/NSE/GRWRHITECH/symbol-2024-12-31.csv'
    dep_file = 'data/NSE/GRWRHITECH/depth-2024-12-31.csv'
    name = "NSE:BAJFINANCE"
    SingleavgParser(symb_file,dep_file,name).start()
    
