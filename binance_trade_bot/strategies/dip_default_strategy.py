import random
import sys
import requests
import json
from datetime import datetime, timedelta
import talib.abstract as ta
import binance_trade_bot.qtpylib.indicators as qtpylib
import pandas as pd
from time import mktime

from binance_trade_bot.auto_trader import AutoTrader
from binance_trade_bot.database import  Coin

class Strategy(AutoTrader):
    def initialize(self):
        super().initialize()
        self.isBacktest = type(self.manager).__name__=='MockBinanceManager'
        self.initialize_current_coin()

        self.data_frames = {}
        self.last_price = {}
        self.add_dataframe_for_all()

    def scout(self):
        """
        Scout for potential jumps from the current coin to another coin
        """
        # check if previous buy order failed. If so, bridge scout for a new coin.
        if self.failed_buy_order:
            self.bridge_scout()

        current_coin = self.db.get_current_coin()
        current_coin_symbol_str = str(current_coin.symbol)
        # Display on the console, the current coin+Bridge, so users can see *some* activity and not think the bot has
        # stopped. Not logging though to reduce log size.
        if True:    print(
        	f"{self.manager.now()} - CONSOLE - INFO - I am scouting the best trades. "
        	f"Current coin: {current_coin + self.config.BRIDGE} ",
        	end="\r",
        )
        
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
                    current_coin, self.config.BRIDGE, self.manager.get_ticker_price(current_coin + self.config.BRIDGE)
                )
                self.logger.info("Ready to start trading")

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