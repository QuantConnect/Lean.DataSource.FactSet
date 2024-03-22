/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Tests;
using QuantConnect.Lean.DataSource.FactSet;
using QuantConnect.Securities;
using System.Collections.Generic;
using QuantConnect.Logging;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    [Explicit("Requires valid FactSet credentials and depends on internet connection")]
    public class FactSetDataProviderHistoryTests
    {
        private readonly FactSetDataProvider _historyDataProvider = new();

        [SetUp]
        public void SetUp()
        {
            _historyDataProvider.Initialize(new HistoryProviderInitializeParameters(null, null, null, null, null, null, null, false, null, null));
        }

        private static IEnumerable<TestCaseData> InvalidHistoryRequestsTestCases
        {
            get
            {
                TestGlobals.Initialize();

                // Invalid security type
                yield return new TestCaseData(Symbols.SPY, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(30));
                yield return new TestCaseData(Symbols.SPX, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(30));
                yield return new TestCaseData(Symbols.SPY_C_192_Feb19_2016, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(30));

                // Canonicals
                var spxCanonicalOption = Symbol.CreateCanonicalOption(Symbols.SPX);
                yield return new TestCaseData(spxCanonicalOption, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(30));

                var spxOption = Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 5150m, new DateTime(2024, 05, 17));

                // Unsupported resolutions
                yield return new TestCaseData(spxOption, Resolution.Hour, TickType.Trade, TimeSpan.FromDays(30));
                yield return new TestCaseData(spxOption, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(30));
                yield return new TestCaseData(spxOption, Resolution.Second, TickType.Trade, TimeSpan.FromDays(30));
                yield return new TestCaseData(spxOption, Resolution.Tick, TickType.Trade, TimeSpan.FromDays(30));

                // Unsupported tick types
                yield return new TestCaseData(spxOption, Resolution.Daily, TickType.Quote, TimeSpan.FromDays(30));
            }
        }

        [TestCaseSource(nameof(InvalidHistoryRequestsTestCases))]
        public void DoesntGetHistoryForInvalidRequests(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var request = CreateHistoryRequest(symbol, resolution, tickType, new DateTime(2024, 03, 20), period);
            var slices = _historyDataProvider.GetHistory(request)?.ToList();

            Assert.That(slices, Is.Null);
        }

        private static IEnumerable<TestCaseData> ValidHistoryRequestsTestCases
        {
            get
            {
                TestGlobals.Initialize();
                var spxOption = Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 5150m, new DateTime(2024, 05, 17));
                var endDate = new DateTime(2024, 03, 20);

                // 30 days until 2023/03/20, starts at 2024/02/19 (a holiday), 22 trading days (23 minus the holiday)
                yield return new TestCaseData(spxOption, Resolution.Daily, TickType.Trade, endDate, TimeSpan.FromDays(30), 22);

                yield return new TestCaseData(spxOption, Resolution.Daily, TickType.OpenInterest, endDate, TimeSpan.FromDays(30), 22);
            }
        }

        [TestCaseSource(nameof(ValidHistoryRequestsTestCases))]
        public void GetsHistory(Symbol symbol, Resolution resolution, TickType tickType, DateTime endDate, TimeSpan period, int expectedHistoryCount)
        {
            var request = CreateHistoryRequest(symbol, resolution, tickType, endDate, period);
            var history = _historyDataProvider.GetHistory(request)?.ToList();

            Assert.That(history, Is.Not.Null.And.Not.Empty);
            Assert.That(history, Has.Count.EqualTo(expectedHistoryCount));
            Assert.That(history, Is.All.InstanceOf(request.DataType));

            if (tickType != TickType.OpenInterest)
            {
                var dataResolutions = history.Select(data => (data.EndTime - data.Time).ToHigherResolutionEquivalent(requireExactMatch: true)).ToList();
                Assert.That(dataResolutions, Is.All.EqualTo(request.Resolution));
            }

            Log.Trace($"History count: {history.Count}");
            foreach (var data in history)
            {
                Log.Trace($"{data.EndTime} - {data}");
            }
        }

        private static HistoryRequest CreateHistoryRequest(Symbol symbol, Resolution resolution, TickType tickType, DateTime endDate, TimeSpan period)
        {
            var dataType = LeanData.GetDataType(resolution, tickType);
            var marketHoursDatabase = MarketHoursDatabase.FromDataFolder();

            var exchangeHours = marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
            var dataTimeZone = marketHoursDatabase.GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

            return new HistoryRequest(
                endDate.Add(-period),
                endDate,
                dataType,
                symbol,
                resolution,
                exchangeHours,
                dataTimeZone,
                resolution,
                true,
                false,
                DataNormalizationMode.Raw,
                tickType);
        }
    }
}
