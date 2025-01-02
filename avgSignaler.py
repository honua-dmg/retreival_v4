import datetime as dt
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import redis
import json
import os
import dotenv
import threading
# what's being saved in redis:
"""
    redlines : string, msg['stonk'].split('-')[0]+'-redLine'
    greenlines : string, msg['stonk'].split('-')[0]+'-greenLine'
    redNotice : string 'stonk'].split('-')[0]+'redNotice'
    SHORTS : stream, msg['stonk'].split('-')[0]+'-short'
    LONGS : stream,  msg['stonk'].split('-')[0]+'-long'

    {"time":traded_time,"ltp":current_ltp,"count":avg_r.xlen(msg['stonk'].split('-')[0])}

"""
def SignalFinder(msg,avg_r:redis.Redis,polariser):
    try:
        red_line_points = avg_r.get(msg['stonk'].split('-')[0]+'-redLine').decode('utf8').split(',')
    except AttributeError:
        red_line_points = []
    try:
        green_line_points = avg_r.get(msg['stonk'].split('-')[0]+'-greenLine').decode('utf8').split(',')
    except AttributeError:
        green_line_points = []

    if avg_r.get(msg['stonk'].split('-')[0]+'redNotice') == None:
        avg_r.set(msg['stonk'].split('-')[0]+'redNotice','true')
    # independant variables
    slice_len =400
    decision_range = 20
    error_range =.3

    try:
        total_buy_qty = float(avg_r.get(msg['stonk'].split('-')[0]+'-total_buy'))
    except Exception:
        total_buy_qty = 0

    # min max calculation
    try:
        slice =sorted([int(float(x[1][b'ltp'])) for x in avg_r.xrevrange(msg['stonk'].split('-')[0],count=slice_len)]) # pretty irritating code, deal with it :)
        maxes = slice[-3:]
        mins = slice[:3]
    except Exception:
        maxes,mins = [],[]
    # current ltp
    current_ltp  = int(float(msg['ltp']))

    # adding ltp to polariser or not
    if current_ltp in polariser.keys():

        polariser[current_ltp] +=msg['vol-buy']
    else:
        polariser[current_ltp] = msg['vol-buy']

    # finding avg qty
    total_buy_qty+=msg['vol-buy']
    avg_r.set(msg['stonk'].split('-')[0]+'-total_buy',total_buy_qty)
    count = len([x for x in polariser.keys() if polariser[x]>0])
    avg_qty = total_buy_qty/count if count>0 else 0# average updates each time we get a new update. 
    # sorted integer ltps
    keys = sorted(polariser.keys())
    # traded timee
    traded_time = msg['last_traded_time']


    # red and green line additions
    if polariser[current_ltp]<=avg_qty:
        red_line_points.append(str(current_ltp))
        green_line_points.append('null')
        if len(green_line_points)>0 and avg_r.get(msg['stonk'].split('-')[0]+'redNotice')==b'false':
            green_line_points[-1] = str(current_ltp)
        avg_r.set(msg['stonk'].split('-')[0]+'redNotice','true')
    else:
        green_line_points.append(str(current_ltp))
        red_line_points.append('null')
        if len(red_line_points)>0 and avg_r.get(msg['stonk'].split('-')[0]+'redNotice')==b'true' :
            red_line_points[-1] = str(current_ltp)
        avg_r.set(msg['stonk'].split('-')[0]+'redNotice','false')

    # setting red and green lines:
    avg_r.set(msg['stonk'].split('-')[0]+'-redLine',','.join(red_line_points))
    avg_r.set(msg['stonk'].split('-')[0]+'-greenLine',','.join(green_line_points))


    # getting integer ltps above and below the current ltp
    below, above  = keys[:keys.index(current_ltp)],keys[keys.index(current_ltp):]
    shortSignal, longSignal = '',''
    india_date=dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")
    with open('./messages/{}-{}.txt'.format(msg['stonk'].split('-')[0],india_date),'a') as f:
        print(f"\n\n\n{current_ltp} ::{traded_time}:: {len(keys)},max:{maxes},mins:{mins} len below, above: {len(below)}, {len(above)},total buy qty: {total_buy_qty},count: {count}, avg qty:{avg_qty} \below:{[polariser[x] for x in below]}, above:{[polariser[x] for x in above]}",file=f)  
        # potential short signal : len(above)<20 and most of them are 0s and below>20 and most of them are reds
        if len(below)>decision_range:
            print('\tchecking for a short',file=f)
            greens,reds = 0,0
            for key in below[-decision_range:]:
                if polariser[key]<=avg_qty:
                    reds+=1
                else:
                    greens+=1
            print(f'\treds:{reds},greens:{greens}',file=f)

            if reds>=decision_range*(1-error_range) and greens/(reds+greens)<=error_range and current_ltp in maxes:
                print(f'SHORT SIGNAL:\n\ttraded_time:{traded_time}: 20+gap found at ltp: {current_ltp},reds:{reds},greens:{greens}\n',file=f)
                shortSignal = '{},{}'.format(current_ltp,avg_r.xlen(msg['stonk'].split('-')[0]))
                # saving SHORT to redis 
                avg_r.xadd(msg['stonk'].split('-')[0]+'-short',{"time":traded_time,"ltp":current_ltp,"count":len(green_line_points)})
        # potential buy signal : short but inverted
        if len(above)>decision_range:
            print('\tchecking for a buy',file=f)
            greens,reds = 0,0
            for key in above[:decision_range]:
                if polariser[key]<=avg_qty:
                    reds+=1
                else:
                    greens+=1
                    
            print(f'\treds:{reds},greens:{greens}',file=f)
            if reds>=decision_range*(1-error_range) and greens/(reds+greens)<=error_range and current_ltp in mins:
                print(f'BUY SIGNAL:\n\ttraded_time:{traded_time} 20+gap found at ltp: {current_ltp},reds:{reds},greens:{greens}\n',file=f)
                buySignal = '{},{}'.format(current_ltp,avg_r.xlen(msg['stonk'].split('-')[0]))
                # saving BUY to redis 
                
                avg_r.xadd(msg['stonk'].split('-')[0]+'-long',{"time":traded_time,"ltp":current_ltp,"count":len(green_line_points)})
    # for reference :)
    avg_r.xadd(msg['stonk'].split('-')[0],msg,maxlen=slice_len*2,approximate=True)
    # for graphing:

    graph_data = {
        'red': ','.join(red_line_points),
        'green': ','.join(green_line_points),
    }
    avg_r.xadd(msg['stonk'].split('-')[0]+'GRAPH',graph_data,maxlen=5,approximate=True)
    #print('added_graph_data to :{}'.format(msg['stonk'].split('-')[0]+'GRAPH'))


import matplotlib.pyplot as plt
from matplotlib.widgets import Button
import numpy as np
import redis
import pandas as pd
import json
from matplotlib.ticker import MultipleLocator
import dotenv
import os


def graphdisv2(testing):
    plt.ion()
    dotenv.load_dotenv()
    
    avg_r = redis.Redis(host="localhost", port="6379", db=2)
    r = redis.Redis(host="localhost", port="6379", db=0)

    _stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
    stonksList = [stonk.split('-')[0] for stonk in _stonksList]
    stonks = {stonk.split('-')[0] + 'GRAPH': '$' for stonk in stonksList}

    # Data structures for graph navigation
    graph_data = {stock: {"red": [], "green": [], "shorts": [], "buys": []} for stock in stonksList}
    current_index = 0

    fig, ax = plt.subplots()
    plt.subplots_adjust(bottom=0.2)

    def update_plot(index):
        """Update the graph for the selected stock."""
        nonlocal current_index
        current_index = index % len(stonksList)
        stock_name = stonksList[current_index]

        # Get data for the current stock
        red = graph_data[stock_name]["red"]
        green = graph_data[stock_name]["green"]
        shorts = graph_data[stock_name]["shorts"]
        buys = graph_data[stock_name]["buys"]

        ax.clear()
        ax.plot(red, color="#B22222", linewidth=1, label="Red")
        ax.plot(green, color="#228B22", linewidth=1, label="Green")

        for short in shorts:
            ax.plot(short[0], short[1], "ro", markersize=5)
        for buy in buys:
            ax.plot(buy[0], buy[1], "bo", markersize=5)

        ax.set_title(f"Graph for {stock_name}")
        ax.xaxis.set_major_locator(MultipleLocator(100))
        ax.yaxis.set_major_locator(MultipleLocator(10))
        ax.legend()
        fig.canvas.draw()

    def next_graph(event):
        update_plot(current_index + 1)

    def prev_graph(event):
        update_plot(current_index - 1)

    axprev = plt.axes([0.1, 0.05, 0.1, 0.075])  # Position: left
    axnext = plt.axes([0.8, 0.05, 0.1, 0.075])  # Position: right
    bnext = Button(axnext, "Next")
    bprev = Button(axprev, "Previous")
    bnext.on_clicked(next_graph)
    bprev.on_clicked(prev_graph)

    # Enter data update loop
    while r.get("end") != b"true":
        data = avg_r.xread(stonks, block=100)
        if not data:
            continue

        for stream in data:
            stock_name = stream[0][:-5].decode()
            msg = {key.decode("utf-8"): value.decode("utf-8") for key, value in stream[1][0][1].items()}

            red = [int(float(x)) if x != "null" else np.nan for x in msg["red"].split(",")]
            green = [int(float(x)) if x != "null" else np.nan for x in msg["green"].split(",")]

            shorts = [
                (int(float(point["count"])), int(float(point["ltp"])))
                for point in (
                    {k.decode("utf-8"): v.decode("utf-8") for k, v in msg.items()}
                    for _, msg in avg_r.xrange(stock_name + "-short")
                )
            ]
            buys = [
                (int(float(point["count"])), int(float(point["ltp"])))
                for point in (
                    {k.decode("utf-8"): v.decode("utf-8") for k, v in msg.items()}
                    for _, msg in avg_r.xrange(stock_name + "-long")
                )
            ]

            graph_data[stock_name] = {"red": red, "green": green, "shorts": shorts, "buys": buys}

            update_plot(current_index)
            plt.pause(0.005)
            plt.grid()
    plt.show()

import os
import csv
import pandas as pd

# work in progress - the only thing that's preventing me from finishing this is them annoying ahh blocks
def signalFinderCSV(msg,avg_r,polariser):
    market,symbol = msg.pop('stonk').split('-')[0].split(':')
    directory = os.path.join('./graphs',market,symbol)
    if not os.path.exists(directory): #checking to see if file path exists                                            # we had to include this try block again due to issues in multiprocessing
        os.makedirs(directory)  
    india_date = dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")
    redgren = os.path.join(directory,f'GraphData-{india_date}.csv')

    if not os.path.exists(redgren):
        with open(redgren,'w') as f:
            writer = csv.DictWriter(f,['red','green']) 


    # red notice
    if avg_r.get(msg['stonk'].split('-')[0]+'redNotice') == None:
        avg_r.set(msg['stonk'].split('-')[0]+'redNotice','true')
    # independant variables
    slice_len =400
    decision_range = 20
    error_range =.3

    try:
        total_buy_qty = float(avg_r.get(msg['stonk'].split('-')[0]+'-total_buy'))
    except Exception:
        total_buy_qty = 0

import redis
import json
import os
import matplotlib.pyplot as plt
from matplotlib.widgets import Button
import numpy as np
import dotenv
from matplotlib.ticker import MultipleLocator

def graphdisv3(testing):
    plt.ion()
    dotenv.load_dotenv()

    avg_r = redis.Redis(host="localhost", port="6379", db=2)
    r = redis.Redis(host="localhost", port="6379", db=0)

    _stonksList = json.loads(os.getenv("STOCKS"))["TEST"] if testing else json.loads(os.getenv("STOCKS"))["REAL"]
    stonksList = [stonk.split('-')[0] for stonk in _stonksList]
    stonks = {stonk.split('-')[0] + 'GRAPH': '$' for stonk in stonksList}

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
        data = avg_r.xread({stock_name + "GRAPH": "$"}, block=100)
        
        if not data:
            return

        for stream in data:
            msg = {key.decode("utf-8"): value.decode("utf-8") for key, value in stream[1][0][1].items()}

            red = [int(float(x)) if x != "null" else np.nan for x in msg["red"].split(",")]
            green = [int(float(x)) if x != "null" else np.nan for x in msg["green"].split(",")]

            shorts = [
                (int(float(point["count"])), int(float(point["ltp"])))
                for point in (
                    {k.decode("utf-8"): v.decode("utf-8") for k, v in msg.items()}
                    for _, msg in avg_r.xrange(stock_name + "-short")
                )
            ]
            buys = [
                (int(float(point["count"])), int(float(point["ltp"])))
                for point in (
                    {k.decode("utf-8"): v.decode("utf-8") for k, v in msg.items()}
                    for _, msg in avg_r.xrange(stock_name + "-long")
                )
            ]

            # Update the current stock's data
            graph_data[stock_name] = {"red": red, "green": green, "shorts": shorts, "buys": buys}

    # Enter data update loop
    while r.get("end") != b"true":
        update_data()  # Fetch and update data only for the current stock
        update_plot()  # Update the plot with the current stock data
        plt.pause(0.005)  # Allow for real-time interaction

    plt.show()

import Main

if __name__=="__main__":
    testing = True
    stremz = threading.Thread(target=Main.SignalWorker,args=(testing,))
    stremz.start()
    graphdisv2(True)
    stremz.join()


