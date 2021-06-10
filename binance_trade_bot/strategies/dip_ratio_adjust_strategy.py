from collections import defaultdict
import random
import sys
from datetime import datetime, timedelta

import requests
import json
import talib.abstract as ta
import binance_trade_bot.qtpylib.indicators as qtpylib
import pandas as pd
from time import mktime

from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.expression import and_

from binance_trade_bot.auto_trader import AutoTrader
from binance_trade_bot.database import Pair, Coin

class Strategy(AutoTrader):
    def initialize(self):
        super().initialize()
        self.isBacktest = type(self.manager).__name__=='MockBinanceManager'
        
        self.initialize_current_coin()
        self.reinit_threshold = self.manager.now().replace(second=0, microsecond=0)
        self.logger.info(f"CAUTION: The ratio_adjust strategy is still work in progress and can lead to losses! Use this strategy only if you know what you are doing, did alot of backtests and can live with possible losses.")
        
        self.data_frames = {}
        self.last_price = {}
        self.add_dataframe_for_all()

    def scout(self):
        #check if previous buy order failed. If so, bridge scout for a new coin.
        if self.failed_buy_order:
            self.bridge_scout()
        
        base_time: datetime = self.manager.now()
        allowed_idle_time = self.reinit_threshold
        if base_time >= allowed_idle_time:
            self.re_initialize_trade_thresholds()
            self.reinit_threshold = self.manager.now().replace(second=0, microsecond=0) + timedelta(minutes=1)

        """
        Scout for potential jumps from the current coin to another coin
        """
        current_coin = self.db.get_current_coin()
        current_coin_symbol_str = str(current_coin.symbol)
        # Display on the console, the current coin+Bridge, so users can see *some* activity and not think the bot has
        # stopped. Not logging though to reduce log size.
        # print(
        #     f"{self.manager.now()} - CONSOLE - INFO - I am scouting the best trades. "
        #     f"Current coin: {current_coin + self.config.BRIDGE} ",
        #     end="\r",
        # )

        current_coin_price = self.manager.get_sell_price(current_coin + self.config.BRIDGE)

        if current_coin_price is None:
            self.logger.info("Skipping scouting... current coin {} not found".format(current_coin + self.config.BRIDGE))
            return

        try:
            if current_coin_symbol_str in self.data_frames:
                if self.isBacktest:
                    idx=self.manager.datetime - (self.manager.datetime - datetime.min) % timedelta(minutes=15)
                    df = self.data_frames[current_coin_symbol_str]['df'].loc[(self.data_frames[current_coin_symbol_str]['df']['date'] == idx)]
                    if df.empty:
                        self.data_frames[current_coin_symbol_str]['df'] = self.get_dataframe(current_coin_symbol_str + self.config.BRIDGE.symbol)
                        self.data_frames[current_coin_symbol_str]['ts'] = self.manager.now()
                        df = self.data_frames[current_coin_symbol_str]['df'].loc[(self.data_frames[current_coin_symbol_str]['df']['date'] == idx)]
                else:
                    self.keep_dataframes_updated()
                    df = self.data_frames[current_coin_symbol_str]['df'].iloc[-1:]
                if not df.empty:
                    current_coin_price = self.manager.get_sell_price(current_coin + self.config.BRIDGE)
                    is_not_on_bridge = self.is_on_bridge() != True
                    if is_not_on_bridge and type(df.values[0][-1]) == bool and df.values[0][-1]:
                        self.logger.info(f"[{self.manager.now()}] detected dip, selling coin waiting for rise")
                        current_coin_price = self.manager.get_sell_price(current_coin + self.config.BRIDGE)
                        if self.manager.sell_alt(current_coin, self.config.BRIDGE, current_coin_price):
                            self.last_price[current_coin_symbol_str] = current_coin_price
                            self.logger.info("sold coin waiting")
                    elif not is_not_on_bridge and type(df.values[0][-2])==bool and df.values[0][-2] and current_coin_symbol_str in self.last_price and current_coin_price < self.last_price[current_coin_symbol_str]:
                        self.logger.info(f"[{self.manager.now()}] detected dip, buying coin waiting for drop")
                        current_coin_price = self.manager.get_sell_price(current_coin + self.config.BRIDGE)
                        if self.manager.buy_alt(current_coin, self.config.BRIDGE, current_coin_price):
                            self.last_price[current_coin_symbol_str] = current_coin_price * (self.manager.get_fee(current_coin,self.config.BRIDGE,False) * 2 + 1)
                            self.logger.info("bought coin back")
        except Exception as e:
            self.logger.info(f"Dip detection failed, due to an error. The main logic of the bot will continue. Error: {e}")

        self._jump_to_best_coin(current_coin, current_coin_price)

    def bridge_scout(self):
        current_coin = self.db.get_current_coin()
        if self.manager.get_currency_balance(current_coin.symbol) > self.manager.get_min_notional(
            current_coin.symbol, self.config.BRIDGE.symbol
        ):
            # Only scout if we don't have enough of the current coin
            return
        new_coin = super().bridge_scout()
        if new_coin is not None:
            self.db.set_current_coin(new_coin)

    def initialize_current_coin(self):
        """
        Decide what is the current coin, and set it up in the DB.
        """
        if self.db.get_current_coin() is None:
            current_coin_symbol = self.config.CURRENT_COIN_SYMBOL
            if not current_coin_symbol:
                current_coin_symbol = random.choice(self.config.SUPPORTED_COIN_LIST)

            self.logger.info(f"Setting initial coin to {current_coin_symbol}")

            if current_coin_symbol not in self.config.SUPPORTED_COIN_LIST:
                sys.exit("***\nERROR!\nSince there is no backup file, a proper coin name must be provided at init\n***")
            self.db.set_current_coin(current_coin_symbol)

            # if we don't have a configuration, we selected a coin at random... Buy it so we can start trading.
            if self.config.CURRENT_COIN_SYMBOL == "":
                current_coin = self.db.get_current_coin()
                self.logger.info(f"Purchasing {current_coin} to begin trading")
                self.manager.buy_alt(
                    current_coin, self.config.BRIDGE, self.manager.get_buy_price(current_coin + self.config.BRIDGE)
                )
                self.logger.info("Ready to start trading")

    def re_initialize_trade_thresholds(self):
        """
        Re-initialize all the thresholds ( hard reset - as deleting db )
        """
        #updates all ratios
        #print('************INITIALIZING RATIOS**********')
        session: Session
        with self.db.db_session() as session:
            c1 = aliased(Coin)
            c2 = aliased(Coin)
            for pair in session.query(Pair).\
                join(c1, and_(Pair.from_coin_id == c1.symbol, c1.enabled == True)).\
                join(c2, and_(Pair.to_coin_id == c2.symbol, c2.enabled == True)).\
                all():
                if not pair.from_coin.enabled or not pair.to_coin.enabled:
                    continue
                #self.logger.debug(f"Initializing {pair.from_coin} vs {pair.to_coin}", False)

                from_coin_price = self.manager.get_sell_price(pair.from_coin + self.config.BRIDGE)
                if from_coin_price is None:
                    # self.logger.debug(
                    #     "Skipping initializing {}, symbol not found".format(pair.from_coin + self.config.BRIDGE),
                    #     False
                    # )
                    continue

                to_coin_price = self.manager.get_buy_price(pair.to_coin + self.config.BRIDGE)
                if to_coin_price is None:
                    # self.logger.debug(
                    #     "Skipping initializing {}, symbol not found".format(pair.to_coin + self.config.BRIDGE),
                    #     False
                    # )
                    continue

                pair.ratio = (pair.ratio *100 + from_coin_price / to_coin_price)  / 101

    def initialize_trade_thresholds(self):
        """
        Initialize the buying threshold of all the coins for trading between them
        """
        session: Session
        with self.db.db_session() as session:
            pairs = session.query(Pair).filter().all()
            grouped_pairs = defaultdict(list)
            for pair in pairs:
                if pair.from_coin.enabled and pair.to_coin.enabled:
                    grouped_pairs[pair.from_coin.symbol].append(pair)

            price_history = {}
            base_date = self.manager.now().replace(second=0, microsecond=0)
            start_date = base_date - timedelta(minutes=200)
            end_date = base_date - timedelta(minutes=1)

            start_date_str = start_date.strftime('%Y-%m-%d %H:%M')
            end_date_str = end_date.strftime('%Y-%m-%d %H:%M')

            self.logger.info(f"Starting ratio init: Start Date: {start_date}, End Date {end_date}")
            for from_coin_symbol, group in grouped_pairs.items():

                if from_coin_symbol not in price_history.keys():
                    price_history[from_coin_symbol] = []
                    for result in  self.manager.binance_client.get_historical_klines(f"{from_coin_symbol}{self.config.BRIDGE_SYMBOL}", "1m", start_date_str, end_date_str, limit=200):
                        price = float(result[1])
                        price_history[from_coin_symbol].append(price)

                for pair in group:                  
                    to_coin_symbol = pair.to_coin.symbol
                    if to_coin_symbol not in price_history.keys():
                        price_history[to_coin_symbol] = []
                        for result in self.manager.binance_client.get_historical_klines(f"{to_coin_symbol}{self.config.BRIDGE_SYMBOL}", "1m", start_date_str, end_date_str, limit=200):                           
                           price = float(result[1])
                           price_history[to_coin_symbol].append(price)

                    if len(price_history[from_coin_symbol]) != 200:
                        self.logger.info(len(price_history[from_coin_symbol]))
                        self.logger.info(f"Skip initialization. Could not fetch last 200 prices for {from_coin_symbol}")
                        continue
                    if len(price_history[to_coin_symbol]) != 200:
                        self.logger.info(f"Skip initialization. Could not fetch last 200 prices for {to_coin_symbol}")
                        continue
                    
                    sma_ratio = 0.0
                    for i in range(100):
                        sma_ratio += price_history[from_coin_symbol][i] / price_history[to_coin_symbol][i]
                    sma_ratio = sma_ratio / 100.0

                    cumulative_ratio = sma_ratio
                    for i in range(100, 200):
                        cumulative_ratio = (cumulative_ratio * 100.0 + price_history[from_coin_symbol][i] / price_history[to_coin_symbol][i]) / 101.0

                    pair.ratio = cumulative_ratio

            self.logger.info(f"Finished ratio init...")

    def update_trade_threshold(self, coin: Coin, coin_price: float):
        pass

    def is_on_bridge(self):
        current_coin_symbol = self.db.get_current_coin().symbol
        current_balance = self.manager.get_currency_balance(current_coin_symbol)
        if current_balance >= self.manager.get_min_notional(current_coin_symbol, self.config.BRIDGE.symbol):
            return False

        return True

    def set_last_price(self, coin: Coin):
        coin_symbol = coin.symbol
        if self.isBacktest:
            self.last_price[coin_symbol]=0
            return

        orders = self.manager.binance_client.get_all_orders(symbol=coin_symbol+self.config.BRIDGE.symbol, limit=1)
        if len(orders)>=1:
            is_not_sell = orders[0]['side'] != 'SELL'
            if is_not_sell:
                self.last_price[coin_symbol]=float(orders[0]['price'])*(self.manager.get_fee(coin,self.config.BRIDGE,False)*2+1)
            else:
                self.last_price[coin_symbol]=float(orders[0]['price'])

    def add_dataframe_for_all(self):        
        curent_coin = self.db.get_current_coin()
        self.set_last_price(curent_coin)       
        for coin in self.config.SUPPORTED_COIN_LIST:
                if coin not in self.data_frames:
                    self.data_frames[coin]={}
                self.data_frames[coin]['df'] = self.get_dataframe(coin + self.config.BRIDGE.symbol)
                self.data_frames[coin]['ts'] = self.manager.now()       

    def keep_dataframes_updated(self):
        for coin in self.config.SUPPORTED_COIN_LIST:
            coin = str(coin)
            if coin not in self.data_frames or ((self.manager.now() - self.data_frames[coin]['ts']).total_seconds() / 60.0) >= 15 if coin != self.db.get_current_coin().symbol else 1:
                if coin not in self.data_frames:
                    self.data_frames[coin]={}
                self.data_frames[coin]['df'] = self.get_dataframe(str(coin)+self.config.BRIDGE.symbol)
                self.data_frames[coin]['ts'] = self.manager.now()

    def get_dataframe(self, ticker_symbol = None, interval ='15m'):
        if ticker_symbol is None:
            ticker_symbol=str(self.db.get_current_coin().symbol)+self.config.BRIDGE.symbol        

        if not self.isBacktest:
            datas = json.loads(requests.get(f'https://www.binance.com/api/v1/klines?interval={interval}&limit=1000&symbol={ticker_symbol}',headers={'user-agent':'Binance/2.30.0 (com.czzhao.binance; build:8; iOS 14.5.1) Alamofire/2.30.0'}).content)
        else:          
            datas = json.loads(requests.get(f'https://www.binance.com/api/v1/klines?interval={interval}&limit=1000&symbol={ticker_symbol}&startTime={int(mktime(self.manager.now().timetuple())*1000)}',headers={'user-agent':'Binance/2.30.0 (com.czzhao.binance; build:8; iOS 14.5.1) Alamofire/2.30.0'}).content)

        data=[]
        for result in datas:
            data.append([float(result[0]),float(result[1]),float(result[2]),float(result[3]),float(result[4]),float(result[5])])
            
        dataframe= pd.DataFrame(data, columns = ['date','open', 'high', 'low','close', 'volume'])
        dataframe['date']=pd.to_datetime(dataframe['date']/1000,unit='s')
        dataframe['esa'] = ta.EMA(dataframe, timeperiod=14)
        dataframe['d'] = ta.EMA(abs(dataframe['close']-dataframe['esa']), timeperiod=21)
        dataframe['ci'] = (dataframe['close'] - dataframe['esa']) / (0.015 * dataframe['d'])
        dataframe['wt1'] = ta.EMA(dataframe['ci'], 21)
        dataframe['wt2'] = ta.EMA(dataframe['wt1'], 4)
        dataframe.loc[(qtpylib.crossed_above(dataframe['wt1'],dataframe['wt2'])),'buySignal']=True
        dataframe.loc[(qtpylib.crossed_below(dataframe['wt1'],dataframe['wt2'])),'sellSignal']=True
        return dataframe
