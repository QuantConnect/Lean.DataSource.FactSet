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
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Lean.DataSource.FactSet;
using QuantConnect.Logging;
using QuantConnect.Tests;
using QuantConnect.Util;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    [Explicit("Requires valid FactSet credentials and depends on internet connection")]
    public class FactSetDataDownloaderTests
    {
        private static IEnumerable<TestCaseData> DataDownloadTestCases
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

        [TestCaseSource(nameof(DataDownloadTestCases))]
        public void DownloadsHistoricalData(Symbol symbol, Resolution resolution, TickType tickType, DateTime endDate, TimeSpan period,
            int expectedHistoryCount)
        {
            using var downloader = new FactSetDataDownloader();

            var parameters = new DataDownloaderGetParameters(symbol, resolution, endDate.Add(-period), endDate, tickType);
            var data = downloader.Get(parameters).ToList();

            Log.Trace("Data points retrieved: " + data.Count);

            Assert.That(data, Is.Not.Null.And.Not.Empty);
            Assert.That(data, Has.Count.EqualTo(expectedHistoryCount));

            var expectedDataType = LeanData.GetDataType(resolution, tickType);
            Assert.That(data, Is.All.InstanceOf(expectedDataType));
        }

        private static IEnumerable<TestCaseData> DataDownloadForCanonicalTestCases
        {
            get
            {
                TestGlobals.Initialize();

                var spx = Symbol.Create("SPX", SecurityType.Index, Market.USA);
                var spxCanonicalOption = Symbol.CreateCanonicalOption(spx);
                yield return new TestCaseData(spxCanonicalOption);

                var spxwCanonicalOption = Symbol.CreateCanonicalOption(spx, "SPXW", Market.USA, null);
                yield return new TestCaseData(spxwCanonicalOption);
            }
        }

        [TestCaseSource(nameof(DataDownloadForCanonicalTestCases))]
        public void DownloadsDataForCanonicalOptionSymbol(Symbol canonical)
        {
            var downloader = new TestableFactSetDataDownloader();
            var parameters = new DataDownloaderGetParameters(canonical, Resolution.Daily,
                new DateTime(2024, 03, 04), new DateTime(2024, 03, 22), TickType.Trade);
            var data = downloader.Get(parameters)?.ToList();

            Assert.That(data, Is.Not.Null.And.Not.Empty);

            Log.Trace("Data points retrieved: " + data.Count);

            var symbolsWithData = data.Select(x => x.Symbol).Distinct().ToList();
            Assert.That(symbolsWithData, Has.Count.GreaterThan(1).And.All.Matches<Symbol>(x => x.Canonical == canonical));

            Assert.That(data, Is.Ordered.By("Time"));
        }

        private class TestableFactSetDataDownloader : FactSetDataDownloader
        {
            protected override IEnumerable<Symbol> GetOptions(Symbol symbol, DateTime startUtc, DateTime endUtc)
            {
                // Let's only take a few contracts from a few days to speed up the test
                return base.GetOptions(symbol, startUtc, startUtc)
                    .GroupBy(x => x.ID.Date)
                    .OrderBy(x => x.Key)
                    .Select(x => x.Take(10))
                    .Take(5)
                    .SelectMany(x => x);
            }
        }
    }
}
