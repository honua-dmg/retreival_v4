import datetime as dt
import redis
import os

def to_terminal(output,dir):
    print(output)


def to_csv(output,dir):
    #print('preparing to save file!')
    if output == None:
        #print('\t no file found ;(')
        return
    
    market,symbol = output.pop('stonk').split('-')[0].split(':')
    directory = os.path.join(dir,market,symbol)
    if not os.path.exists(directory): #checking to see if file path exists                                            # we had to include this try block again due to issues in multiprocessing
        os.makedirs(directory)  
    india_date = dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")
    file_path = os.path.join(directory,f'AVGPARSED-{india_date}.csv')


    with open(file_path,'a') as f:

        if os.path.getsize(file_path) == 0:
            print('file doesn not exist, making one now')
            f.write(','.join([key for key in sorted(output.keys())]))
            f.write("\n")
        f.write(','.join([str(output[x])for x  in sorted(output.keys()) ]))
        f.write('\n')
    #print('\t saved to file!')
                

def parseMsg(r,message): # r stands for the redis client
    """
    args:
        r : redis client
        dir : directory where the files will be saved
    returns:
        output: a dict containing all the data that'll be sent to the excel sheet 
                    keys:
                        last traded time
                        ltp
                        vol traded

                        buy price [if buy round ltp to integer]
                        buy vol   [delta vol]
                        but amt   [ltp * buy_vol]
                        cumulative buy amount                   - redis
                        cumulative buy vol                      -redis
                        buy average [cum_buy_amt/cum_buy_vol]   
                        sequence                                - redis
        
                        sell price [if buy round ltp to integer]
                        sell vol   [delta vol]
                        sell amt   [ltp * buy_vol]
                        cumulative sell amount
                        cumulative sell vol
                        sell average [cum_sell_amt/cum_sell_vol]
                        sequence

                or None no message is in queue 
                        if there's no change in vol traded
                        if bid price is not yet recorded
                        if message relates to Depth

    """
    output = {} 


    #{'bid_price1': 3066.15, 'bid_price2': 3065.45, 'bid_price3': 3065.35, 'bid_price4': 3065.2, 'bid_price5': 3065.15, 'ask_price1': 3067.7, 'ask_price2': 3067.85, 'ask_price3': 3067.95, 'ask_price4': 3068.0, 'ask_price5': 3068.05, 'bid_size1': 4, 'bid_size2': 2, 'bid_size3': 2, 'bid_size4': 3, 'bid_size5': 3, 'ask_size1': 9, 'ask_size2': 59, 'ask_size3': 1, 'ask_size4': 1, 'ask_size5': 10, 'bid_order1': 1, 'bid_order2': 1, 'bid_order3': 1, 'bid_order4': 2, 'bid_order5': 1, 'ask_order1': 2, 'ask_order2': 3, 'ask_order3': 1, 'ask_order4': 1, 'ask_order5': 1, 'type': 'dp', 'symbol': 'BSE:PIDILITIND-A'}
    #{'ltp': 879.7, 'vol_traded_today': 4686533, 'last_traded_time': 1721106718, 'exch_feed_time': 1721106718, 'bid_size': 77, 'ask_size': 571, 'bid_price': 879.6, 'ask_price': 879.7, 'last_traded_qty': 1, 'tot_buy_qty': 667534, 'tot_sell_qty': 1232010, 'avg_trade_price': 881.73, 'low_price': 877.0, 'high_price': 888.1, 'lower_ckt': 0, 'upper_ckt': 0, 'open_price': 882.3, 'prev_close_price': 881.35, 'type': 'sf', 'symbol': 'NSE:SBIN-EQ', 'ch': -1.65, 'chp': -0.1872}
    file_symbol = ''.join(['-' if x == ":" else x for x in message['symbol']])
    if message.pop('type') == 'sf': # ltp message
        #print('\n symbol recieved')

        last_traded_vol = r.get(f'{file_symbol}-vol_traded_today')
         # essentially you can't subtract a Nonetype from an integer
        if last_traded_vol == None:
            r.set(f'{file_symbol}-vol_traded_today',message['vol_traded_today']) 
            return None

        delta_vol = int(float(message['vol_traded_today']))-int(float(last_traded_vol))

        if delta_vol ==0:
            #print('no delta vol')
            return None
        
        #print('delta vol hai!')
        #print(f'\t\t{delta_vol}')
        r.set(f'{file_symbol}-vol_traded_today',message['vol_traded_today']) # resetting volume traded 
        
        try:
            buy = bid(float(message['ltp']),r,file_symbol)
        except ValueError as e:
            #print(f'no bids recieved yet! exception:{e}')
            return None
        if buy==None :
            return
        if buy:# figuring out weather if was buy or sell
            #print('buy recieved!')
            buy_sell_avg("buy",r,file_symbol,message,delta_vol,output) 
        else:
            #print('sell hai!')
            buy_sell_avg("sell",r,file_symbol,message,delta_vol,output) 
            

        #print(output)
        return output
        



    else: # depth message received 
        #print('\n depth recieved')
        # sending bid_prices and ask_prices recieved from message to cache as a string 'bid1-bid2-bid3-bid4-bid5'
        r.set(f'{file_symbol}-bid','-'.join([str(x) for x in [message[f'bid_price{i}'] for i in range(1,6)]]))
        r.set(f'{file_symbol}-ask','-'.join([str(x) for x in [message[f'ask_price{i}'] for i in range(1,6)]]))
        #print(r.get(f'{file_symbol}-bid').decode().split('-'))
        #print(r.get(f'{file_symbol}-ask').decode().split('-'))
        # bid prices in list format >>> [message[f'bid_price{i}'] for i in range(1,6)] 
        #print("bid prices saved!")
        return None

def bid(ltp,r,file_symbol):
    """
    args:
        ltp: last traded price
        r  : redis client
        file_symbol: the name of the symbol in question

    returns:
        True if its a bid that's closer to the ltp
        False if its a ask that's closer to the ltp
    """
    if r.get(f'{file_symbol}-bid') == None or r.get(f'{file_symbol}-ask') == None:
        raise ValueError # no values found yet
    try:
        #print(r.get(f'{file_symbol}-bid').decode().split('-'))
        #print(r.get(f'{file_symbol}-ask').decode().split('-'))
        bid_diff = sum([(ltp-float(x))**2 for x in r.get(f'{file_symbol}-bid').decode().split('-')])/5
        sum_diff = sum([(ltp-float(x))**2 for x in r.get(f'{file_symbol}-ask').decode().split('-')])/5
    except Exception as e:
        #print(f"differenciating between bid and ask failed: ltp: {ltp}, exception:{e}")
        raise ValueError
    
    return bid_diff<sum_diff

def buy_sell_avg(type,r,file_symbol,message,delta_vol,output):
    """
    args:
        type        : buy or sell?
        r           : redis client
        file_symbol : the name of the symbol in question
        message     : input message (from symbol )
        delta_vol   : change in volume traded
        output      : final returned dict 

    """
    order = ['buy','sell'] if type=='buy' else ['sell','buy']
    # *** amount 
    amount = float(message['ltp'])*delta_vol # amount bought or sold
    
    #cumulative-vol
    if r.get(f'{file_symbol}-cumulative_vol-{order[0]}')!= None:#checking a value exists in cache
        cumulative_vol = int(r.get(f'{file_symbol}-cumulative_vol-{order[0]}'))+delta_vol
    else:
        cumulative_vol = delta_vol

    r.set(f'{file_symbol}-cumulative_vol-{order[0]}',cumulative_vol)
        

    #cumulative-amt
    if r.get(f'{file_symbol}-cumulative_amt-{order[0]}')!=None: #checking a value exists in cache                      #cumulative-buy-amt
        cumulative_amt =float(r.get(f'{file_symbol}-cumulative_amt-{order[0]}'))+amount
    else:
        cumulative_amt = amount

    r.set(f'{file_symbol}-cumulative_amt-{order[0]}',cumulative_amt)

    # average
    average = int(cumulative_amt/cumulative_vol)

    # getting previous avg
    if r.get(f'{file_symbol}-avg-{order[0]}')!=None:#checking a value exists in cache
        prev_avg = int(r.get(f'{file_symbol}-avg-{order[0]}'))
    else:
        prev_avg = 0
    print(f'\tprev avg{prev_avg}, current avg:{average}')
    # sequence
    if r.get(f'{file_symbol}-sequence-{order[0]}')!=None: #checking a value exists in cache
        print('sequence found in cache')

        sequence = int(r.get(f'{file_symbol}-sequence-{order[0]}'))
        if prev_avg != average:
            sequence +=1


    else:
        sequence =1




    r.set(f'{file_symbol}-avg-{order[0]}',average) 
    r.set(f'{file_symbol}-sequence-{order[0]}',sequence)
    print(f'{order[0]}:{sequence}')
    output['ltp'] = message['ltp']
    output['vol_traded_today'] = message['vol_traded_today']
    output['last_traded_time'] = dt.datetime.strftime(dt.datetime.fromtimestamp(float(message['last_traded_time'])),"%H-%M-%S")

    output[f'price-{order[0]}']     = int(float(message['ltp']))
    output[f'vol-{order[0]}']       = delta_vol
    output[f'amt-{order[0]}']       = amount
    output[f'average-{order[0]}']   = average 
    output[f'sequence-{order[0]}']  = sequence
    output[f'cumulative-vol-{order[0]}'] = cumulative_vol
    output[f'cumulative-amt-{order[0]}'] = cumulative_amt


    output[f'price-{order[1]}']     = 0
    output[f'vol-{order[1]}']       = 0
    output[f'amt-{order[1]}']       = 0
    
    average = r.get(f'{file_symbol}-avg-{order[1]}')

    
    sequence = r.get(f'{file_symbol}-sequence-{order[1]}')

    
    cumulative_vol = r.get(f'{file_symbol}-cumulative_vol-{order[1]}')

    
    cumulative_amt = r.get(f'{file_symbol}-cumulative_amt-{order[1]}')

    if average        ==None:
        average = 0
    if sequence       ==None:
        sequence = 1
    if cumulative_vol ==None:
        cumulative_vol = 0   
    if cumulative_amt ==None:
        cumulative_amt = 0

    output[f'average-{order[1]}']   = int(average)
    output[f'sequence-{order[1]}']  = int(sequence)
    output[f'cumulative_vol-{order[1]}'] = int(cumulative_vol)
    output[f'cumulative_amt-{order[1]}'] = float(cumulative_amt)

    output['stonk'] = message['symbol']

        