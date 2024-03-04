﻿# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from AlgorithmImports import *

### <summary>
### Example algorithm using the custom data type as a source of alpha
### </summary>
class CustomDataAlgorithm(QCAlgorithm):
    def Initialize(self):
        ''' Initialise the data and resolution required, as well as the cash and start-end dates for your algorithm. All algorithms must initialized.'''
        
        self.SetStartDate(2020, 10, 7)   #Set Start Date
        self.SetEndDate(2020, 10, 11)    #Set End Date
        self.equity_symbol = self.AddEquity("SPY", Resolution.Daily).Symbol
        self.custom_data_symbol = self.AddData(MyCustomDataType, self.equity_symbol).Symbol

    def OnData(self, slice):
        ''' OnData event is the primary entry point for your algorithm. Each new data point will be pumped in here.

        :param Slice slice: Slice object keyed by symbol containing the stock data
        '''
        data = slice.Get(MyCustomDataType)
        if data:
            custom_data = data[self.custom_data_symbol]
            if custom_data.SomeCustomProperty == "buy":
                self.SetHoldings(self.equitySymbol, 1)
            elif custom_data.SomeCustomProperty == "sell":
                self.SetHoldings(self.equitySymbol, -1)

    def OnOrderEvent(self, orderEvent):
        ''' Order fill event handler. On an order fill update the resulting information is passed to this method.

        :param OrderEvent orderEvent: Order event details containing details of the events
        '''
        if orderEvent.Status == OrderStatus.Filled:
            self.Debug(f'Purchased Stock: {orderEvent.Symbol}')
