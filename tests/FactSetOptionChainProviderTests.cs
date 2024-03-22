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
using QuantConnect.Util;
using QuantConnect.Lean.DataSource.FactSet;
using QuantConnect.Securities;
using System.Collections.Generic;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Logging;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    [Explicit("Requires valid FactSet credentials and depends on internet connection")]
    public class FactSetOptionChainProviderTests
    {
        private FactSetApi _api;
        private FactSetOptionChainProvider _optionChainProvider;

        [OneTimeSetUp]
        public void SetUp()
        {
            var factSetAuthConfigurationStr = Config.Get("factset-auth-config");
            var factSetAuthConfiguration = JsonConvert.DeserializeObject<FactSet.SDK.Utils.Authentication.Configuration>(factSetAuthConfigurationStr);
            var symbolMapper = new FactSetSymbolMapper();
            _api = new FactSetApi(factSetAuthConfiguration, symbolMapper);
            _optionChainProvider = new FactSetOptionChainProvider(_api);
        }

        private static Symbol[] Underlyings => new (string Ticker, SecurityType SecurityType)[]
            {
                ("SPY", SecurityType.Equity),
                ("AAPL", SecurityType.Equity),
                ("IBM", SecurityType.Equity),
                ("GOOG", SecurityType.Equity),
                ("SPX", SecurityType.Index),
                ("VIX", SecurityType.Index),
                ("DAX", SecurityType.Index),
            }
            .Select(t => Symbol.Create(t.Ticker, t.SecurityType, Market.USA))
            .ToArray();

        [TestCaseSource(nameof(Underlyings))]
        public void GetsOptionChainGivenTheUnderlyingSymbol(Symbol underlying)
        {
            GetOptionChain(underlying);
        }

        private List<Symbol> GetOptionChain(Symbol symbol, DateTime? reference = null)
        {
            var referenceDate = reference ?? DateTime.UtcNow.Date.AddDays(-1);
            var optionChain = _optionChainProvider.GetOptionContractList(symbol, referenceDate).ToList();

            Assert.That(optionChain, Is.Not.Null.And.Not.Empty);

            // Multiple strikes
            var strikes = optionChain.Select(x => x.ID.StrikePrice).Distinct().ToList();
            Assert.That(strikes, Has.Count.GreaterThan(1).And.All.GreaterThan(0));

            // Multiple expirations
            var expirations = optionChain.Select(x => x.ID.Date).Distinct().ToList();
            Assert.That(expirations, Has.Count.GreaterThan(1).And.All.GreaterThanOrEqualTo(referenceDate.Date));

            // All contracts have the same underlying
            var underlying = symbol.Underlying ?? symbol;
            Assert.That(optionChain.Select(x => x.Underlying), Is.All.EqualTo(underlying));

            Log.Trace($"Option chain for {symbol} contains {optionChain.Count} contracts");

            return optionChain;
        }
    }
}
