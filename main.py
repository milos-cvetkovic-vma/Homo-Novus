import csv

from six import reraise
from bitmex_websocket_custom import BitMEXWebsocket
import traceback
from flask import Flask, render_template, request, redirect
import sqlite3
import threading
import datetime
import json
import bitmex
from subscriptions import MY_SUBS, DEFAULT_SUBS
import time
import gc

config_continue_next_hour = True
config_consecutive_penalty_points = True

endpoints = {
  'production': 'wss://ws.bitmex.com/realtime ',
  'testnet': 'wss://ws.testnet.bitmex.com/realtime'
}

ORDER_SYMBOL = 0
ORDER_QUANTITY = 1
ORDER_PERCENTAGE = 2
ORDER_HOURS = 3
ORDER_DURATIONS = 4
ORDER_CYCLE = 5
ORDER_STATE_G = 6
ORDER_STATE_R = 7
ORDER_ACTIVE = 8
ORDER_PENALTY_POINTS = 9

BACKUP_INCREASE = 0.005
SLEEP_BETWEEN_ITERATIONS = 0.6
TIME_FROM_PARTIAL = 3

PRICE_FIX = 15.0 # Empirically determined, when BTC price was 41-43k
PRICE_CORRECTION = 0.5 # On all limit orders sent to the exchange.

max_penalty_points = 3

rounding_constant = 0.5
def round_to(n, precision):
    correction =0.5 if n>=0 else -0.5
    return int(n/precision+correction)*precision

import sys
g_is_test = True if sys.argv[1] == "True" else False
g_symbol = sys.argv[2]
g_api_key = None
g_api_secret = None
if len(sys.argv) > 3:
    g_api_key = sys.argv[3]
    g_api_secret = sys.argv[4]


app = Flask(__name__)

conn = sqlite3.connect('stocks.db', check_same_thread=False) # TODO: switch to in-memory db, no need to recover after program crash, too complicated at this point
c = conn.cursor() # TODO: this name is in conflict with local name? Maybe it can cause race-conditions.

# Create table
c.execute('''CREATE TABLE IF NOT EXISTS orders
             (symbol text, quantity text, percentages text, hours text, durations text, cycle integer, state_g integer, state_r integer, active bit, penalty_points integer)''')

#c.execute('''DELETE FROM orders''')


#commit the changes to db			
conn.commit()


# c = conn.cursor()
# c.execute('''INSERT INTO orders VALUES ('XBTUSD', 100, '',
#               '12;13', '1;1', 0, -1, -1, 1, 0) ''')	
# conn.commit()

class Order:
    def __init__(self):
        self.action = None
        self.totalQuantity = None
        self.orderType = None
        self.price = None

class Contract:
    def __init__(self):
        self.symbol = None

class TradingStatus:
    def __init__(self):
        self.percs_dict = None
        self.quantity = None
        self.basePrice = None
        self.currentPosition = 0
        self.r1Price = None
        self.g1Price = None
        self.cycle = 0
        self.r1Filled = False
        self.g1Filled = False
        self.r1Id = None
        self.g1Id = None
        self.r2Id = None
        self.g2Id = None
        self.r2BackupId = None
        self.g2BackupId = None
        self.r3Id = None
        self.g3Id = None
        self.r4Id = None
        self.g4Id = None
        self.buyStop = None
        self.sellStop = None
        self.r1Manual = False
        self.g1Manual = False
        self.symbol = None
        #self.lastKnownMidPrice = None
        self.positionPrice = None
        self.penalty_points = 0
        self.penalty_active = False
        self.execIDs = []

class AlgoThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(AlgoThread, self).__init__(*args, **kwargs)
        self.percs_data = {}
        self.symbolToTradingStatus = {}
        self.logger = None
        self.stop_for_the_day = False
        self.new_day = False
        self.reset_day = False
        self.tradingStatusMutex = threading.Lock()
        with open("percentages.csv", encoding='utf-8') as csvf:

            csvReader = csv.DictReader(csvf)
            
            # Convert each row into a dictionary
            # and add it to data
            for row in csvReader:
                key = row['Hour']
                for val in row:
                    row[val] = float(row[val])
                self.percs_data[key] = row

    def run(self,*args,**kwargs):
        self.loop()

    def retriable_submit(self, f):
        cond = False
        retried = False
        while not cond:
            try:
                if retried:
                    self.tradingStatusMutex.acquire()
                    self.logger.info("Connecting")
                    self.client = bitmex.bitmex(test=g_is_test, api_key=g_api_key, api_secret=g_api_secret)
                    self.logger.info("Connected")
                r = f()
                cond = True
                return r
            except:
                # We are assuming every submit is within the mutex. We release it while we sleep before the retry.
                retried = True
                self.tradingStatusMutex.release()
                self.logger.info("Retryable error")
                self.logger.error('Error occured')
                #self.logger.flush()
                self.logger.error(traceback.format_exc())
                self.logger.info("Retrying")
                time.sleep(1)

    def loop(self):
        global is_test
        global symbol
        global api_key
        global api_secret
        global conn
        self.logger = self.setup_logger()
        conn.set_trace_callback(self.logger.info)
        ws = None
        #ticker_old = None
        self.client = None
        time_to_refresh_in_seconds = 60
        previous_refresh_time = time.time()
        prev_time = None
        new_time = None

        # Run forever
        states_per_cycle = 2 # 2 states for each cycle, active and ended
        try:
            current_day1 = None
            prev_current_day = None
            while(True): # TODO: while not stopped
                if self.reset_day:
                    # In case new_day didn't reset because of failures, we need to keep track of this with reset_day.
                    self.reset_day = False
                    self.new_day = False
                else:
                    prev_current_day = current_day1
                    current_day1 = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
                    if current_day1 != prev_current_day and prev_current_day != None:
                        # TODO: It still doesn't work if we put trading hours at the end of the day,
                        # we should avoid trading in last period, and in the first period of the day
                        # (e.g., 11pm and 12am with 1-hour durations, or 10pm and 12am with 2-hour durations).
                        self.new_day = True
                        self.reset_day = False
                        self.stop_for_the_day = False
                        self.logger.info("New day")
                    elif self.stop_for_the_day:
                        self.logger.info("Not active today: " + str(current_day1))
                        time.sleep(30)
                        continue

                if ws is None or ws.ws is None or ws.ws.sock is None:
                    try:
                        if ws is not None and ws.ws is not None:
                            ws_ptr = ws
                            ws = None
                            ws_ptr.exit()
                        ws = BitMEXWebsocket(endpoint=endpoints['production'] if not g_is_test else endpoints['testnet'],
                                    symbol=g_symbol, callback=self.on_update,
                                    api_key=g_api_key, api_secret=g_api_secret,
                                    subscriptions=MY_SUBS)
                        #ws.get_instrument()
                        #self.client = bitmex.bitmex(test=g_is_test, api_key=g_api_key, api_secret=g_api_secret)
                        #self.logger.info("Instrument data: %s" % )
                    except:
                        self.logger.error('Error occured')
                        #self.logger.flush()
                        self.logger.error(traceback.format_exc())
                        time.sleep(1)
                        continue

                prev_time = new_time
                new_time = time.time()
                if prev_time != None:
                    self.logger.info("Iter time: " + str(new_time - prev_time))
                if (new_time - previous_refresh_time) >= time_to_refresh_in_seconds or self.client == None:
                    previous_refresh_time = time.time()
                    self.tradingStatusMutex.acquire()
                    self.logger.info("Connecting")
                    try:
                        self.client = bitmex.bitmex(test=g_is_test, api_key=g_api_key, api_secret=g_api_secret)
                    except:
                        self.logger.error('Error occured')
                        #self.logger.flush()
                        self.logger.error(traceback.format_exc())
                        time.sleep(1)
                        continue
                    finally:
                        self.tradingStatusMutex.release()
                    self.logger.info("Connected")

                # ticker_new = ws.get_ticker_both_round_and_not()
                # if ticker_new != ticker_old:
                #     self.logger.info('Price update: ' + str(ticker_new))
                # ticker_old = ticker_new

                # TODO stop program when exception happens here
                sleepTimeInSeconds = SLEEP_BETWEEN_ITERATIONS
                c = conn.cursor()
                active_orders = c.execute("SELECT * FROM orders WHERE active = 1").fetchall()
                conn.commit()
                assert(len(active_orders) <= 1) # TODO: do not support multiple active orders for now. We will stop all trading if single active order has penalties.
                for order in active_orders:
                    # Percs is like this {r1:<x>, r2:<x2>, r3:<x3>, g1: ...}:{r1:...}
                    percs = []
                    symbol = g_symbol
                    tradingStatus = None
                    if symbol not in self.symbolToTradingStatus:
                        tradingStatus = TradingStatus()
                        tradingStatus.symbol = symbol
                        self.symbolToTradingStatus[symbol] = tradingStatus
                    tradingStatus = self.symbolToTradingStatus[symbol]
                    hours = order[ORDER_HOURS].split(';')
                    for hour in hours:
                        perc_hour = self.percs_data[hour]
                        percs.append(json.dumps(perc_hour))
                    durations = order[ORDER_DURATIONS].split(';')
                    quantity = order[ORDER_QUANTITY]
                    self.tradingStatusMutex.acquire()

                    penalty_points = None
                    try:
                        tradingStatus.penalty_points = order[ORDER_PENALTY_POINTS]

                        if self.new_day:
                            self.reset_day = True
                            self.logger.info("New day reset")
                            tradingStatus.cycle = 0
                            self.resetPenaltyPoints(tradingStatus)
                        
                        penalty_points = tradingStatus.penalty_points
                    finally: 
                        self.tradingStatusMutex.release()

                    if (penalty_points >= max_penalty_points):
                        self.logger.info("Stop for the day")
                        self.stop_for_the_day = True
                        break

                    max_cycle = len(hours) * states_per_cycle
                    self.logger.debug(hours)
                    current_time = datetime.datetime.now()
                    current_day = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
                    self.logger.debug(current_time)

                    # We don't put stop orders due to slippage, but detect price here, and put market orders.
                    if tradingStatus.sellStop or tradingStatus.buyStop or (config_continue_next_hour and (tradingStatus.r1Manual or tradingStatus.g1Manual)):
                        # Get current market price for the tick.
                        self.logger.info("Getting quote for stop orders")
                        
                        self.tradingStatusMutex.acquire()
                        # double lock
                        try:
                            if tradingStatus.sellStop or tradingStatus.buyStop or (config_continue_next_hour and (tradingStatus.r1Manual or tradingStatus.g1Manual)):
                                # TODO: do retriable submit for all self.client. calls.
                                res = self.retriable_submit(lambda: self.client.Quote.Quote_get(symbol=symbol, reverse=True, count=1).result())
                                assert res[1].status_code == 200
                                resj = json.loads(res[1].text)
                                quote = resj[-1]
                                askPrice = quote['askPrice']
                                bidPrice = quote['bidPrice']
                                mid = (float(bidPrice) + float(askPrice)) / 2
                                #tradingStatus.lastKnownMidPrice = mid
                                self.logger.info("Current quote: " + str(quote) + ". Calculated mid price: " + str(mid))
                                if tradingStatus.buyStop:
                                    self.logger.info("Buy stop: " + str(tradingStatus.buyStop['stopPx']) + ' for ' + str(tradingStatus.buyStop['orderQty']))
                                    if tradingStatus.buyStop['stopPx'] <= askPrice:
                                        self.logger.info("r2 market order placing")
                                        res = self.client.Order.Order_new(
                                            symbol=tradingStatus.buyStop['symbol'],
                                            orderQty=tradingStatus.buyStop['orderQty'],
                                            ordType='Market').result()
                                        assert res[1].status_code == 200
                                        self.logger.info("r2 market order placed")
                                        self.r2Filled(tradingStatus, tradingStatus.currentPosition)
                                        tradingStatus.buyStop = None
                                elif tradingStatus.sellStop:
                                    self.logger.info("Sell stop: " + str(tradingStatus.sellStop['stopPx']) + ' for ' + str(tradingStatus.sellStop['orderQty']))
                                    if tradingStatus.sellStop['stopPx'] >= bidPrice:
                                        self.logger.info("g2 market order placing")
                                        res = self.client.Order.Order_new(
                                            symbol=tradingStatus.sellStop['symbol'],
                                            orderQty=tradingStatus.sellStop['orderQty'],
                                            ordType='Market').result()
                                        assert res[1].status_code == 200
                                        self.logger.info("g2 market order placed")
                                        self.g2Filled(tradingStatus, -tradingStatus.currentPosition)
                                        tradingStatus.sellStop = None
                                elif config_continue_next_hour and (tradingStatus.r1Manual or tradingStatus.g1Manual): # and tradingStatus.currentPosition != 0:
                                    self.logger.info("Current position: " + str(tradingStatus.currentPosition) + ', r1Manual: ' + str(tradingStatus.g1Manual))
                                    # Check for g1.
                                    if tradingStatus.g1Manual and mid <= tradingStatus.g1Price:
                                        tradingStatus.g1Manual = False
                                        self.g1Filled(tradingStatus)
                                    # Check for r1.
                                    elif tradingStatus.r1Manual and mid >= tradingStatus.r1Price:
                                        tradingStatus.r1Manual = False
                                        self.r1Filled(tradingStatus)
                        finally:
                            self.tradingStatusMutex.release()

                    # if ws.api_key:
                    #     logger.info("Funds: %s" % ws.funds())
                    # logger.info("Market Depth: %s" % ws.market_depth())
                    # logger.info("Recent Trades: %s\n\n" % ws.recent_trades())
                    contract = Contract()
                    contract.symbol = symbol

                    def getPrices(i, quantity, tradingStatus, percs):
                        currentPrice = tradingStatus.basePrice
                        percs_dict = json.loads(percs[-i])
                        #Create order objects
                        orderBuy = Order()
                        orderBuy.action = 'BUY'
                        orderBuy.totalQuantity = float(quantity)
                        orderBuy.orderType = 'LMT'
                        orderBuy.price = currentPrice - (currentPrice*percs_dict['g1']/100)
                        orderBuy.price = round_to(orderBuy.price, rounding_constant)
                        tradingStatus.g1Price = orderBuy.price

                        orderSell = Order()
                        orderSell.action = 'SELL'
                        orderSell.totalQuantity = float(quantity)
                        orderSell.orderType = 'LMT'
                        orderSell.price = currentPrice + (currentPrice*percs_dict['r1']/100)
                        orderSell.price = round_to(orderSell.price, rounding_constant)
                        tradingStatus.r1Price = orderSell.price

                        return orderBuy, orderSell

                    #toBlock = False
                    i = 0
                    for hour in reversed(hours):
                        i = i + 1
                        percs_dict = json.loads(percs[-i])
                        hour_i = int(hour)
                        duration = int(durations[-i])
                        end_t = current_day + datetime.timedelta(hours=hour_i+duration)
                        hour_t = current_day + datetime.timedelta(hours=hour_i)
                        currentIterationCycle = max_cycle - i * states_per_cycle
                        if tradingStatus.cycle >= currentIterationCycle + 2:
                            continue
                        elif tradingStatus.cycle >= currentIterationCycle + 1:
                            if current_time >= end_t:
                                self.logger.info('End cycle, exit positions')
                                self.tradingStatusMutex.acquire()
                                try:
                                    # Check again.
                                    if tradingStatus.cycle >= currentIterationCycle + 1:
                                        tradingStatus.cycle += 1
                                        for oid in [
                                                    tradingStatus.r1Id,
                                                    tradingStatus.r2Id,
                                                    tradingStatus.r3Id,
                                                    tradingStatus.r4Id,
                                                    tradingStatus.g1Id,
                                                    tradingStatus.g2Id,
                                                    tradingStatus.g3Id,
                                                    tradingStatus.g4Id,
                                                    tradingStatus.g2BackupId,
                                                    tradingStatus.r2BackupId
                                                    ]:
                                            try:
                                                if oid is not None:
                                                    res = self.client.Order.Order_cancel(orderID=oid).result()
                                                    assert res[1].status_code == 200
                                            except:
                                                # Perhaps unnecessary, cancel should be idempotent
                                                self.logger.info('Failed to cancel order: ' + str(oid))

                                        # maximum_absolute_loss = 300 # TODO: change strategy for determining penalty point here, to be relative to the price (and gr1?)
                                        # if tradingStatus.currentPosition != 0:
                                        #     assert tradingStatus.positionPrice != None
                                        #     loss = 0
                                        #     if tradingStatus.currentPosition > 0: # short
                                        #         loss = tradingStatus.lastKnownMidPrice - tradingStatus.positionPrice
                                        #     else: # long
                                        #         loss = tradingStatus.positionPrice - tradingStatus.lastKnownMidPrice
                                            
                                        #     if loss > maximum_absolute_loss:
                                        #         self.addPenaltyPoint(tradingStatus)

                                        last_hour_of_the_day = False
                                        # if i == 1:
                                        #     last_hour_of_the_day = True

                                        # if tradingStatus.penalty_points >= max_penalty_points:
                                        #     last_hour_of_the_day = True
                                        # else:
                                        if config_consecutive_penalty_points and not tradingStatus.penalty_active:
                                            self.resetPenaltyPoints(tradingStatus)

                                        if tradingStatus.currentPosition != 0 and (not config_continue_next_hour or last_hour_of_the_day):
                                            self.logger.info("Placing/closing orders")
                                            res = self.client.Order.Order_new(symbol=symbol, orderQty=tradingStatus.currentPosition, ordType='Market').result()
                                            assert res[1].status_code == 200
                                        
                                        prev_cycle = tradingStatus.cycle
                                        prev_position = tradingStatus.currentPosition
                                        prev_symbol = tradingStatus.symbol
                                        tradingStatus = TradingStatus()
                                        tradingStatus.symbol = prev_symbol
                                        tradingStatus.cycle = prev_cycle
                                        if config_continue_next_hour and not last_hour_of_the_day:
                                            tradingStatus.currentPosition = prev_position
                                        self.symbolToTradingStatus[symbol] = tradingStatus
                                finally:
                                    self.tradingStatusMutex.release()
                                sleepTimeInSeconds = 0
                                break
                        elif tradingStatus.cycle >= currentIterationCycle and current_time >= hour_t: # TODO: handle making order inactive, after all cycles.
                            # Place order pair and increment state in database.
                            # Break the loop.

                            if current_time >= end_t:
                                tradingStatus.cycle += 1
                                continue

                            # Get current market price for the tick.
                            self.logger.info("Getting quote")
                            res = self.client.Quote.Quote_get(symbol=g_symbol, reverse=True, count=1).result()
                            assert res[1].status_code == 200
                            resj = json.loads(res[1].text)
                            quote = resj[-1]
                            askPrice = quote['askPrice']
                            bidPrice = quote['bidPrice']
                            mid = (float(bidPrice) + float(askPrice)) / 2
                            self.logger.info("Current quote: " + str(quote) + ". Calculated mid price: " + str(mid))
                            self.logger.info("Trading status: " + str(json.dumps(tradingStatus.__dict__)))

                            tradingStatus.basePrice = mid
                            tradingStatus.percs_dict = percs_dict
                            tradingStatus.quantity = float(quantity)
                            tradingStatus.penalty_active = False

                            (orderBuy, orderSell) = getPrices(i, quantity, tradingStatus, percs)
                            tradingStatus.cycle += 1
                            
                            self.tradingStatusMutex.acquire()
                            try:
                                # g1
                                #Place orders
                                # First put safeties, in case anything crashes. We will cancel them at the end of the hour, or when we exit position.
                                self.logger.info('g2 safety start')
                                price = tradingStatus.basePrice - (tradingStatus.basePrice*tradingStatus.percs_dict['g2']/100) + PRICE_FIX
                                price = round_to(price, rounding_constant)
                                self.putG2Safety(price, tradingStatus)
                                self.logger.info('g2 safety end')

                                self.logger.info('r2 safety start')
                                price = tradingStatus.basePrice + (tradingStatus.basePrice*tradingStatus.percs_dict['r2']/100) - PRICE_FIX
                                price = round_to(price, rounding_constant)
                                self.putR2Safety(price, tradingStatus)
                                self.logger.info('r2 safety end')

                                if not config_continue_next_hour or tradingStatus.currentPosition == 0:
                                    self.logger.info("Placing orders. g1: " + str(orderBuy.price) + " - r1: " + str(orderSell.price))
                                    res = self.client.Order.Order_new(symbol=symbol, orderQty=orderBuy.totalQuantity, price=orderBuy.price + PRICE_CORRECTION, ordType='Limit').result()
                                    assert res[1].status_code == 200
                                    resj = json.loads(res[1].text)
                                    tradingStatus.g1Id = resj['orderID']
                                    # r1 # TODO: maybe should put this in separate critical section, to avoid placing one if first is executed immediately, thus risking the other one to be executed.
                                    res = self.client.Order.Order_new(symbol=symbol, orderQty=-orderSell.totalQuantity, price=orderSell.price - PRICE_CORRECTION, ordType='Limit').result()
                                    assert res[1].status_code == 200
                                    resj = json.loads(res[1].text)
                                    tradingStatus.r1Id = resj['orderID']
                                    self.logger.info("Orders placed")
                                else:
                                    self.logger.info("Current position: " + str(tradingStatus.currentPosition))
                                    self.logger.info("Placing orders. g1: " + str(orderBuy.price) + " - r1: " + str(orderSell.price))

                                    if tradingStatus.currentPosition > 0:
                                        #self.r1Filled(tradingStatus)
                                        tradingStatus.r1Manual = True
                                        res = self.client.Order.Order_new(symbol=symbol, orderQty=orderBuy.totalQuantity+tradingStatus.currentPosition, price=orderBuy.price + PRICE_CORRECTION, ordType='Limit').result()
                                        assert res[1].status_code == 200
                                        resj = json.loads(res[1].text)
                                        tradingStatus.g1Id = resj['orderID']
                                        self.logger.info("Order g1 placed")
                                        # If previous position was partially filled, here we may want to add limit orders as well...
                                        if tradingStatus.currentPosition < tradingStatus.quantity:
                                            res = self.client.Order.Order_new(symbol=symbol, orderQty=-orderSell.totalQuantity+tradingStatus.currentPosition, price=orderSell.price - PRICE_CORRECTION, ordType='Limit').result()
                                            assert res[1].status_code == 200
                                            resj = json.loads(res[1].text)
                                            tradingStatus.r1Id = resj['orderID']
                                            self.logger.info("Partial order r1 placed")
                                    elif tradingStatus.currentPosition < 0:
                                        #self.g1Filled(tradingStatus)
                                        tradingStatus.g1Manual = True
                                        res = self.client.Order.Order_new(symbol=symbol, orderQty=-orderSell.totalQuantity+tradingStatus.currentPosition, price=orderSell.price - PRICE_CORRECTION, ordType='Limit').result()
                                        assert res[1].status_code == 200
                                        resj = json.loads(res[1].text)
                                        tradingStatus.r1Id = resj['orderID']
                                        self.logger.info("Order r1 placed")
                                        if tradingStatus.currentPosition > -tradingStatus.quantity:
                                            res = self.client.Order.Order_new(symbol=symbol, orderQty=orderBuy.totalQuantity+tradingStatus.currentPosition, price=orderBuy.price + PRICE_CORRECTION, ordType='Limit').result()
                                            assert res[1].status_code == 200
                                            resj = json.loads(res[1].text)
                                            tradingStatus.g1Id = resj['orderID']
                                            self.logger.info("Partial order g1 placed")

                                    sleepTimeInSeconds = 0
                            finally:
                                    self.tradingStatusMutex.release()

                            break
                        self.logger.debug(hour_t)
                processing_time = time.time() - new_time
                if processing_time <= sleepTimeInSeconds:
                    time.sleep(sleepTimeInSeconds - processing_time)
                else:
                    # yield
                    time.sleep(0.001)
        except:
            self.logger.error('Error occured')
            #self.logger.flush()
            toPrint = traceback.format_exc()
            self.logger.error(toPrint)
            #self.logger.flush()
            print(toPrint)

            conn.set_trace_callback(None)

            import logging
            logging.shutdown()
            
            reraise

    def on_update(self, item):
        assert item['ordStatus'] == 'Filled' or item['ordStatus'] == 'PartiallyFilled'
        symbol = item['symbol']
        execId = item['execID']
        tradingStatus = self.symbolToTradingStatus[symbol]
        self.logger.info('On update: ' + str(item))
        # TODO: This only calculates price for the last portion, if it was partially filled first, it won't account for that.
        # Maybe we should use 'avgPx'
        #tradingStatus.positionPrice = item['price']

        # TODO: It can happen that r1 and g1 are filled, and we go into both paths...
        self.tradingStatusMutex.acquire()
        try:
            if execId in tradingStatus.execIDs:
                return
            tradingStatus.execIDs.append(execId)
            lastQty = item['lastQty']
            if (item['ordStatus'] == 'Filled' or item['ordStatus'] == 'PartiallyFilled') and lastQty:
                #cumQty = item['cumQty']
                #leavesQty = item['leavesQty']
                self.logger.info('Filled')
                if item['orderID'] == tradingStatus.r1Id:
                    tradingStatus.currentPosition += lastQty
                    self.r1Filled(tradingStatus)
                elif item['orderID'] == tradingStatus.g1Id:
                    tradingStatus.currentPosition -= lastQty
                    self.g1Filled(tradingStatus)
                elif item['orderID'] == tradingStatus.r2Id:
                    self.r2Filled(tradingStatus, lastQty, False)
                elif item['orderID'] == tradingStatus.g2Id:
                    self.g2Filled(tradingStatus, lastQty, False)
                elif item['orderID'] == tradingStatus.r3Id:
                    self.logger.info('r3 filled')
                    tradingStatus.currentPosition -= lastQty

                    # cancel r1 in case it was only partially filled before.
                    if tradingStatus.r1Id:
                        self.logger.info('r1 cancel start')
                        res = self.client.Order.Order_cancel(orderID=tradingStatus.r1Id).result()
                        assert res[1].status_code == 200
                        tradingStatus.r1Id = None
                        self.logger.info('r1 cancel finish')

                    # put r4 at r1 price, sell limit
                    self.logger.info('r4 start')
                    price = tradingStatus.basePrice + (tradingStatus.basePrice*tradingStatus.percs_dict['r1']/100)
                    price = round_to(price, rounding_constant)
                    quantity = tradingStatus.currentPosition
                    if tradingStatus.currentPosition <= 0:
                        if not tradingStatus.r4Id:
                            if tradingStatus.currentPosition < 0:
                                res = self.client.Order.Order_new(symbol=symbol, orderQty=quantity, price=price - PRICE_CORRECTION, ordType='Limit').result()
                                assert res[1].status_code == 200
                                resj = json.loads(res[1].text)
                                tradingStatus.r4Id = resj['orderID']

                            # cancel r2
                            self.logger.info('r2 cancel start')
                            if (tradingStatus.r2Id):
                                res = self.client.Order.Order_cancel(orderID=tradingStatus.r2Id).result()
                                assert res[1].status_code == 200
                                tradingStatus.r2Id = None
                            elif tradingStatus.buyStop:
                                tradingStatus.buyStop = None
                            self.logger.info('r2 cancel finish')
                        else:
                            res = self.client.Order.Order_amend(orderID=tradingStatus.r4Id, orderQty=quantity).result()
                            assert res[1].status_code == 200
                    elif tradingStatus.currentPosition > 0:
                        # Here we have to put buy stop order.
                        tradingStatus.buyStop['orderQty'] = tradingStatus.currentPosition
                    self.logger.info('r4 finish')

                    # put g2, sell stop
                    self.logger.info('g2 start')
                    price = tradingStatus.basePrice - (tradingStatus.basePrice*tradingStatus.percs_dict['g2']/100) + PRICE_FIX
                    price = round_to(price, rounding_constant)
                    if tradingStatus.currentPosition < 0:
                        # r3 will always have to be executed fully before price goes to g2 sell stop (because limit orders are price barriers).
                        # TODO: put orderQty here always at max then?
                        tradingStatus.sellStop = dict(symbol=symbol, orderQty=tradingStatus.currentPosition, stopPx=price)
                    # r3 is already buy limit...
                    # elif tradingStatus.currentPosition > 0: # Put g2 buy limit
                    self.logger.info('g2 finish')
                elif item['orderID'] == tradingStatus.g3Id:
                    self.logger.info('g3 filled')
                    tradingStatus.currentPosition += lastQty

                    # cancel g1 in case it was only partially filled before.
                    if tradingStatus.g1Id:
                        self.logger.info('g1 cancel start')
                        res = self.client.Order.Order_cancel(orderID=tradingStatus.g1Id).result()
                        assert res[1].status_code == 200
                        tradingStatus.g1Id = None
                        self.logger.info('g1 cancel finish')

                    # put g4 at g1 price, buy limit
                    self.logger.info('g4 start')
                    price = tradingStatus.basePrice - (tradingStatus.basePrice*tradingStatus.percs_dict['g1']/100)
                    price = round_to(price, rounding_constant)
                    quantity = tradingStatus.currentPosition
                    if tradingStatus.currentPosition >= 0:
                        if not tradingStatus.g4Id:
                            if tradingStatus.currentPosition > 0:
                                res = self.client.Order.Order_new(symbol=symbol, orderQty=quantity, price=price + PRICE_CORRECTION, ordType='Limit').result()
                                assert res[1].status_code == 200
                                resj = json.loads(res[1].text)
                                tradingStatus.g4Id = resj['orderID']

                            # cancel g2
                            self.logger.info('g2 cancel start')
                            if tradingStatus.g2Id:
                                res = self.client.Order.Order_cancel(orderID=tradingStatus.g2Id).result()
                                assert res[1].status_code == 200
                                tradingStatus.g2Id = None
                            elif tradingStatus.sellStop:
                                tradingStatus.sellStop = None
                            self.logger.info('g2 cancel finish')
                        else:
                            res = self.client.Order.Order_amend(orderID=tradingStatus.g4Id, orderQty=quantity).result()
                            assert res[1].status_code == 200
                    elif tradingStatus.currentPosition < 0:
                        # Here we have to put sell stop order.
                        tradingStatus.sellStop['orderQty'] = tradingStatus.currentPosition
                    self.logger.info('g4 finish')

                    # put r2, buy stop
                    self.logger.info('r2 start')
                    price = tradingStatus.basePrice + (tradingStatus.basePrice*tradingStatus.percs_dict['r2']/100) - PRICE_FIX
                    price = round_to(price, rounding_constant)
                    if tradingStatus.currentPosition > 0:
                        tradingStatus.buyStop = dict(symbol=symbol, orderQty=tradingStatus.currentPosition, stopPx=price)
                    self.logger.info('r2 finish')
                elif item['orderID'] == tradingStatus.r4Id:
                    self.logger.info('r4 filled')
                    tradingStatus.currentPosition += lastQty

                    if tradingStatus.currentPosition == 0:
                        # cancel g2
                        self.logger.info('g2 cancel start')
                        if tradingStatus.g2Id:
                            res = self.client.Order.Order_cancel(orderID=tradingStatus.g2Id).result()
                            assert res[1].status_code == 200 
                            tradingStatus.g2Id = None
                        elif tradingStatus.sellStop:
                            tradingStatus.sellStop = None 
                        self.logger.info('g2 cancel finish')
                    else:
                        tradingStatus.sellStop['orderQty'] = tradingStatus.currentPosition

                    # cancel remaining r3
                    self.logger.info('r3 remaining cancel finish')
                    if tradingStatus.r3Id:
                        res = self.client.Order.Order_cancel(orderID=tradingStatus.r3Id).result()
                        assert res[1].status_code == 200
                    elif tradingStatus.buyStop:
                        tradingStatus.buyStop = None
                    self.logger.info('r3 cancel finish')

                    
                    # cancel backups/safeties
                    self.cancelSafeties(tradingStatus)

                    if config_consecutive_penalty_points and tradingStatus.currentPosition == 0:
                        self.resetPenaltyPoints(tradingStatus)
                elif item['orderID'] == tradingStatus.g4Id:
                    self.logger.info('g4 filled')
                    tradingStatus.currentPosition -= lastQty

                    if tradingStatus.currentPosition == 0:
                        # cancel r2
                        self.logger.info('r2 cancel finish')
                        if tradingStatus.r2Id:
                            res = self.client.Order.Order_cancel(orderID=tradingStatus.r2Id).result()
                            assert res[1].status_code == 200
                        elif tradingStatus.buyStop:
                            tradingStatus.buyStop = None
                        self.logger.info('r2 cancel finish')
                    else:
                        tradingStatus.buyStop['orderQty'] = tradingStatus.currentPosition


                    # cancel remaining g3
                    self.logger.info('g3 remaining cancel finish')
                    if tradingStatus.g3Id:
                        res = self.client.Order.Order_cancel(orderID=tradingStatus.g3Id).result()
                        assert res[1].status_code == 200
                    elif tradingStatus.sellStop:
                        tradingStatus.sellStop = None
                    self.logger.info('g3 cancel finish')

                    # cancel backups/safeties
                    self.cancelSafeties(tradingStatus)

                    if config_consecutive_penalty_points and tradingStatus.currentPosition == 0:
                        self.resetPenaltyPoints(tradingStatus)
                elif item['orderID'] == tradingStatus.g2BackupId:
                    tradingStatus.g2BackupId = None
                    tradingStatus.sellStop = None
                    self.g2Filled(tradingStatus, lastQty)
                elif item['orderID'] == tradingStatus.r2BackupId:
                    tradingStatus.r2BackupId = None
                    tradingStatus.buyStop = None
                    self.r2Filled(tradingStatus, lastQty)
        finally:
            self.tradingStatusMutex.release()

    def r1Filled(self, tradingStatus):
        self.logger.info('r1 filled')
        tradingStatus.r1Filled = True

        if tradingStatus.currentPosition >= 0:
            tradingStatus.g1Manual = False
            tradingStatus.r1Manual = False
            if tradingStatus.currentPosition > 0:
                # cancel g1
                self.logger.info('cancel g1 start')
                if tradingStatus.g1Id:
                    res = self.client.Order.Order_cancel(orderID=tradingStatus.g1Id).result()
                    assert res[1].status_code == 200
                self.logger.info('cancel g1 finish')

                # put r2, buy stop
                self.logger.info('r2 start')
                price = tradingStatus.basePrice + (tradingStatus.basePrice*tradingStatus.percs_dict['r2']/100) - PRICE_FIX
                price = round_to(price, rounding_constant)
                tradingStatus.buyStop = dict(symbol=symbol, orderQty=tradingStatus.currentPosition, stopPx=price)
                self.logger.info('r2 finish')

                # put r3, double, buy limit
                self.logger.info('r3 start')
                quantity = tradingStatus.quantity+tradingStatus.currentPosition
                if not tradingStatus.r3Id:
                    price = tradingStatus.r1Price - (tradingStatus.r1Price*tradingStatus.percs_dict['r3']/100)
                    price = round_to(price, rounding_constant)
                    res = self.client.Order.Order_new(symbol=symbol, orderQty=quantity, price=price + PRICE_CORRECTION, ordType='Limit').result()
                    assert res[1].status_code == 200
                    resj = json.loads(res[1].text)
                    tradingStatus.r3Id = resj['orderID']
                else:
                    res = self.client.Order.Order_amend(orderID=tradingStatus.r3Id, orderQty=quantity).result()
                    assert res[1].status_code == 200
                self.logger.info('r3 finish')
        else:
            if tradingStatus.g1Id:
                res = self.client.Order.Order_amend(orderID=tradingStatus.g1Id, orderQty=tradingStatus.quantity+tradingStatus.currentPosition).result()
                assert res[1].status_code == 200

    def g1Filled(self, tradingStatus):
        self.logger.info('g1 filled')
        tradingStatus.g1Filled = True

        if tradingStatus.currentPosition <= 0:
            tradingStatus.r1Manual = False
            tradingStatus.g1Manual = False
            if tradingStatus.currentPosition < 0:
                # cancel r1
                self.logger.info('r1 cancel start')
                if tradingStatus.r1Id:
                    res = self.client.Order.Order_cancel(orderID=tradingStatus.r1Id).result()
                    assert res[1].status_code == 200
                    tradingStatus.r1Id = None
                self.logger.info('r1 cancel finish')

                # put g2, sell stop
                self.logger.info('g2 start')
                price = tradingStatus.basePrice - (tradingStatus.basePrice*tradingStatus.percs_dict['g2']/100) + PRICE_FIX
                price = round_to(price, rounding_constant)
                tradingStatus.sellStop = dict(symbol=symbol, orderQty=tradingStatus.currentPosition, stopPx=price)
                self.logger.info('g2 finish')        

                # put g3, double, sell limit
                self.logger.info('g3 start')
                quantity = -tradingStatus.quantity+tradingStatus.currentPosition
                if not tradingStatus.g3Id:
                    price = tradingStatus.g1Price + (tradingStatus.g1Price*tradingStatus.percs_dict['g3']/100)
                    price = round_to(price, rounding_constant)
                    res = self.client.Order.Order_new(symbol=symbol, orderQty=quantity, price=price - PRICE_CORRECTION, ordType='Limit').result()
                    assert res[1].status_code == 200
                    resj = json.loads(res[1].text)
                    tradingStatus.g3Id = resj['orderID']
                else:
                    res = self.client.Order.Order_amend(orderID=tradingStatus.g3Id, orderQty=quantity).result()
                    assert res[1].status_code == 200
                self.logger.info('g3 finish')
        else:
            if tradingStatus.r1Id:
                res = self.client.Order.Order_amend(orderID=tradingStatus.r1Id, orderQty=-tradingStatus.quantity+tradingStatus.currentPosition).result()
                assert res[1].status_code == 200

    def putG2Safety(self, stop_price, tradingStatus):
        price = stop_price
        backupPrice = price - BACKUP_INCREASE*price
        backupPrice = round_to(backupPrice, rounding_constant)
        res = self.client.Order.Order_new(symbol=symbol, orderQty=-tradingStatus.quantity, stopPx=backupPrice, ordType='Stop').result()
        assert res[1].status_code == 200
        resj = json.loads(res[1].text)
        tradingStatus.g2BackupId = resj['orderID']

    def putR2Safety(self, stop_price, tradingStatus):
        price = stop_price
        backupPrice = price + BACKUP_INCREASE*price
        backupPrice = round_to(backupPrice, rounding_constant)
        res = self.client.Order.Order_new(symbol=symbol, orderQty=tradingStatus.quantity, stopPx=backupPrice, ordType='Stop').result()
        assert res[1].status_code == 200
        resj = json.loads(res[1].text)
        tradingStatus.r2BackupId = resj['orderID']

    def r2Filled(self, tradingStatus, lastQty, addPenalty=True):
        self.logger.info('r2 filled')
        tradingStatus.currentPosition -= lastQty

        tradingStatus.sellStop = None
        tradingStatus.buyStop = None
        toAddPenalty = False

        # cancel r1 in case it was only partially filled before.
        if tradingStatus.r1Id:
            self.logger.info('r1 cancel start')
            res = self.client.Order.Order_cancel(orderID=tradingStatus.r1Id).result()
            assert res[1].status_code == 200
            tradingStatus.r1Id = None
            self.logger.info('r1 cancel finish')

        # cancel r3
        if tradingStatus.r3Id:
            # r2 can be entered from 2 states, so it may be that r3 wasn't set
            self.logger.info('r3 cancel start')
            res = self.client.Order.Order_cancel(orderID=tradingStatus.r3Id).result()
            assert res[1].status_code == 200
            tradingStatus.r3Id = None
            toAddPenalty = True
            self.logger.info('r3 cancel finish')
                    
        # cancel g4
        if tradingStatus.g4Id:
            self.logger.info('g4 cancel start')
            res = self.client.Order.Order_cancel(orderID=tradingStatus.g4Id).result()
            assert res[1].status_code == 200
            tradingStatus.g4Id = None
            toAddPenalty = True
            self.logger.info('g4 cancel finish')

        # cancel remaining g3
        if tradingStatus.g3Id:
            self.logger.info('r3 remaining cancel start')
            res = self.client.Order.Order_cancel(orderID=tradingStatus.g3Id).result()
            assert res[1].status_code == 200
            tradingStatus.g3Id = None
            self.logger.info('g3 cancel finish')

        # cancel backups/safeties
        self.cancelSafeties(tradingStatus)

        if addPenalty:
            if toAddPenalty:
                self.addPenaltyPoint(tradingStatus)
        else:
            if tradingStatus.currentPosition == 0:
                self.resetPenaltyPoints(tradingStatus)

    def cancelSafeties(self, tradingStatus):
        if tradingStatus.r2BackupId:
            self.logger.info('r2Backup cancel start')
            res = self.client.Order.Order_cancel(orderID=tradingStatus.r2BackupId).result()
            assert res[1].status_code == 200
            tradingStatus.r2BackupId = None
            self.logger.info('r2Backup cancel finish')
        if tradingStatus.g2BackupId:
            self.logger.info('g2Backup cancel start')
            res = self.client.Order.Order_cancel(orderID=tradingStatus.g2BackupId).result()
            tradingStatus.g2BackupId = None
            assert res[1].status_code == 200
            self.logger.info('g2Backup cancel finish')

    def g2Filled(self, tradingStatus, lastQty, addPenalty=True):
        # HERE
        # TODO: it's better to just cancel all (but careful not to cancel market order put before g2Filled is called). We can use cancelAll API.
        # TODO: In other places, there's cancel with multiple order IDs, to cancel in bulk.
        # Correct safeties? Perhaps there's no need, if program crashed, all other orders would stay as well, hence they would be fully filled.
        self.logger.info('g2 filled')
        tradingStatus.currentPosition += lastQty

        tradingStatus.sellStop = None
        tradingStatus.buyStop = None
        toAddPenalty = False

        # cancel g1 in case it was only partially filled before.
        if tradingStatus.g1Id:
            self.logger.info('g1 cancel start')
            res = self.client.Order.Order_cancel(orderID=tradingStatus.g1Id).result()
            assert res[1].status_code == 200
            tradingStatus.g1Id = None
            self.logger.info('g1 cancel finish')

        # cancel g3
        if tradingStatus.g3Id:
            # g2 can be entered from 2 states, so it may be that g3 wasn't set. Checking because g3Id can be None, but operation is idempotent.
            self.logger.info('g3 cancel start')
            res = self.client.Order.Order_cancel(orderID=tradingStatus.g3Id).result()
            assert res[1].status_code == 200
            tradingStatus.g3Id = None
            toAddPenalty = True
            self.logger.info('g3 cancel finish')
                    
        # cancel r4
        if tradingStatus.r4Id:
            self.logger.info('r4 cancel start')
            res = self.client.Order.Order_cancel(orderID=tradingStatus.r4Id).result()
            assert res[1].status_code == 200
            tradingStatus.r4Id = None
            toAddPenalty = True
            self.logger.info('r4 cancel finish')

        # cancel remaining r3
        if tradingStatus.r3Id:
            self.logger.info('r3 remaining cancel start')
            res = self.client.Order.Order_cancel(orderID=tradingStatus.r3Id).result()
            assert res[1].status_code == 200
            tradingStatus.r3Id = None
            self.logger.info('r3 cancel finish')

        # cancel backups/safeties
        self.cancelSafeties(tradingStatus)

        if addPenalty:
            if toAddPenalty:
                self.addPenaltyPoint(tradingStatus)
        else:
            if tradingStatus.currentPosition == 0:
                self.resetPenaltyPoints(tradingStatus)

    def addPenaltyPoint(self, tradingStatus):
        global conn
        c = conn.cursor()
        c.execute('''UPDATE orders SET penalty_points = penalty_points+1 WHERE symbol = ?''', (tradingStatus.symbol,))
        conn.commit()
        tradingStatus.penalty_points += 1
        tradingStatus.penalty_active = True

    def resetPenaltyPoints(self, tradingStatus):
        global conn
        c = conn.cursor()
        c.execute('''UPDATE orders SET penalty_points = 0 WHERE symbol = ?''', (tradingStatus.symbol,))
        conn.commit()
        tradingStatus.penalty_points = 0

    def setup_logger(self):
        # Prints logger info to terminal
        import os
        import logging.handlers

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
        ch = logging.StreamHandler()
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # add formatter to ch
        ch.setFormatter(formatter)
        filename = "log.txt"
        should_roll_over = os.path.isfile(filename)
        fileHandler = logging.handlers.RotatingFileHandler(filename, backupCount=20)
        if should_roll_over:  # log already exists, roll over!
            fileHandler.doRollover()
        #fileHandler = logging.FileHandler("{0}/{1}.log".format(logPath, fileName))
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)
        logger.addHandler(ch)
        return logger

        
@app.route("/")
def index():
    global conn
    c = conn.cursor()
    active_rows = c.execute("SELECT * FROM orders WHERE active = 1").fetchall()
    conn.commit()
    c = conn.cursor()
    inactive_rows = c.execute("SELECT * FROM orders WHERE active <> 1").fetchall()
    conn.commit()

    return render_template('index.html', active_rows = active_rows, inactive_rows = inactive_rows)

@app.route('/addorder', methods=['POST'])
def my_form_post():
    global conn
    symbol = request.form['symbol']
    qty = request.form['quantity']
    perc = request.form['wick percentages']
    hours = request.form['trigger hours']
    durations = request.form['durations']
    insertStmt = '''INSERT INTO orders
             VALUES('%(symbol)s', '%(qty)s', '%(perc)s', '%(hours)s', '%(durations)s', 0, -1, -1, 1, 0)''' % {'symbol':symbol, 'qty':qty, 'perc':perc, 'hours':hours, 'durations':durations}
    print(insertStmt + '\n')
    c = conn.cursor()
    c.execute(insertStmt)
    conn.commit()
    
    return redirect("/")

if __name__ == '__main__':
    algo = AlgoThread()
    algo.start()
    app.run()
    print("Stopping\n")
    algo.join()
    #close the connection
    conn.close()