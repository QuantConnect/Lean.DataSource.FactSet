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
using QuantConnect.Tests;
using QuantConnect.Lean.DataSource.FactSet;
using QuantConnect.Securities;
using System.Collections.Generic;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Logging;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class FactSetSymbolMapperTests
    {
        private static IEnumerable<(string, Symbol)> SymbolConvertionTestCases()
        {
            // Equity
            yield return new ("SPY", Symbols.SPY);

            // Index
            yield return new ("SPX", Symbols.SPX);

            // Options
            var msft = Symbol.Create("MSFT", SecurityType.Equity, Market.USA);
            var msftOption = Symbol.CreateOption(msft, Market.USA, OptionStyle.American, OptionRight.Call, 26m, new DateTime(2009, 11, 21));

            yield return new ("MSFT#091121C00026000", msftOption);
            // Index Options

            var spxOption = Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Put, 5210m, new DateTime(2024, 03, 28));
            yield return new ("SPX#240328P05210000", spxOption);
        }

        private static IEnumerable<TestCaseData> FactSetToLeanSymbolConversionTestCases()
        {
            return SymbolConvertionTestCases().Select(x => new TestCaseData(x.Item1, x.Item2));
        }

        [TestCaseSource(nameof(FactSetToLeanSymbolConversionTestCases))]
        public void ConvertsFactSetOCC21SymbolToLeanSymbol(string factSetOcc21Symbol, Symbol expectedLeanSymbol)
        {
            var mapper = new FactSetSymbolMapper();
            var optionStyle = expectedLeanSymbol.SecurityType.IsOption() ? expectedLeanSymbol.ID.OptionStyle : OptionStyle.American;
            var leanSymbol = mapper.ParseFactSetOCC21Symbol(factSetOcc21Symbol, expectedLeanSymbol.SecurityType, expectedLeanSymbol.ID.Market,
                optionStyle);
            Assert.That(leanSymbol, Is.EqualTo(expectedLeanSymbol));
        }

        private static IEnumerable<TestCaseData> LeanToFactSetSymbolConversionTestCases()
        {
            return SymbolConvertionTestCases().Select(x => new TestCaseData(x.Item2, x.Item1));
        }

        [TestCaseSource(nameof(LeanToFactSetSymbolConversionTestCases))]
        public void ConvertsLeanSymbolToFactSetOCC21Symbol(Symbol leanSymbol, string expectedFactSetOcc21Symbol)
        {
            var mapper = new FactSetSymbolMapper();
            var factSetOcc21Symbol = mapper.GetBrokerageSymbol(leanSymbol);
            Assert.That(factSetOcc21Symbol, Is.EqualTo(expectedFactSetOcc21Symbol));
        }

        private static IEnumerable<(string, Symbol)> OptionFosSymbolConvertionTestCases()
        {
            // Options
            var aapl = Symbol.Create("AAPL", SecurityType.Equity, Market.USA);
            var aaplOption = Symbol.CreateOption(aapl, Market.USA, OptionStyle.American, OptionRight.Call, 65m, new DateTime(2024, 06, 21));

            yield return new ("AAPL.US#C229V", aaplOption);
            // Index Options

            var spxOption = Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 5100m, new DateTime(2024, 12, 20));
            yield return new ("SPX.SPX#C06W1", spxOption);
        }

        private static IEnumerable<TestCaseData> OptionLeanSymbolToFactSetFosConversionTestCases()
        {
            return OptionFosSymbolConvertionTestCases().Select(x => new TestCaseData(x.Item2, x.Item1));
        }

        [TestCaseSource(nameof(OptionLeanSymbolToFactSetFosConversionTestCases))]
        [Explicit("Requires valid FactSet credentials and depends on internet connection")]
        public void ConvertsOptionLeanSymbolToFosSymbol(Symbol leanOptionSymbol, string expectedFosSymbol)
        {
            var mapper = new TestableFactSetSymbolMapper();
            using var api = GetApi(mapper);
            mapper.SetApi(api);

            var fosSymbol = mapper.GetFactSetFosSymbol(leanOptionSymbol);

            Log.Trace(fosSymbol);

            Assert.That(fosSymbol, Is.EqualTo(expectedFosSymbol));
        }

        private static IEnumerable<TestCaseData> OptionFosSymbolToLeanSymbolConversionTestCases()
        {
            return OptionFosSymbolConvertionTestCases().Select(x => new TestCaseData(x.Item1, x.Item2));
        }

        [TestCaseSource(nameof(OptionFosSymbolToLeanSymbolConversionTestCases))]
        [Explicit("Requires valid FactSet credentials and depends on internet connection")]
        public void ConvertsOptionFosSymbolToLeanSymbol(string fosSymbol, Symbol expectedLeanOptionSymbol)
        {
            var mapper = new TestableFactSetSymbolMapper();
            using var api = GetApi(mapper);
            mapper.SetApi(api);

            var leanOptionSymbol = mapper.GetLeanSymbolFromFactSetFosSymbol(fosSymbol, expectedLeanOptionSymbol.SecurityType);

            Assert.That(leanOptionSymbol, Is.EqualTo(expectedLeanOptionSymbol));
        }

        [Test]
        [Explicit("Requires valid FactSet credentials and depends on internet connection")]
        public void ConvertsOptionsFosSymbolsToLeanSymbolsInBatch()
        {
            var mapper = new TestableFactSetSymbolMapper();
            using var api = GetTestableApi(mapper, batchSize: 2);
            mapper.SetApi(api);

            var fosSymbols = new List<string>
            {
                "SPX.SPX#C06W1",
                "SPX.SPX#C09K8",
                "SPX.SPX#C0CC3",
                "SPX.SPX#C0QVS",
                "SPX.SPX#C0SHF",
                "SPX.SPX#C0TG2",
                "SPX.SPX#C0WPR",
                "SPX.SPX#C0WWT",
                "SPX.SPX#C1NTH",
                "SPX.SPX#C1QKQ",
                "SPX.SPX#P04VY",
                "SPX.SPX#P0JN8",
                "SPX.SPX#P0MZR",
                "SPX.SPX#P0PT3",
                "SPX.SPX#P0QML",
                "SPX.SPX#P13YS",
                "SPX.SPX#P175V",
                "SPX.SPX#P195K",
                "SPX.SPX#P1CDN",
                "SPX.SPX#P1H59",
            };

            var expectedLeanSymbols = new List<Symbol>
            {
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 5100m, new DateTime(2024, 12, 20)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 4800m, new DateTime(2025, 12, 19)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 5000m, new DateTime(2025, 12, 19)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 5200m, new DateTime(2025, 12, 19)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 6000m, new DateTime(2025, 12, 19)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 6200m, new DateTime(2025, 12, 19)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 5000m, new DateTime(2024, 12, 20)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 3000m, new DateTime(2024, 12, 20)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 5100m, new DateTime(2026, 12, 18)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Call, 8400m, new DateTime(2024, 12, 20)),

                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Put, 3700m, new DateTime(2026, 12, 18)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Put, 5700m, new DateTime(2024, 12, 20)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Put, 2800m, new DateTime(2026, 12, 18)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Put, 3600m, new DateTime(2025, 12, 19)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Put, 2800m, new DateTime(2025, 12, 19)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Put, 3800m, new DateTime(2024, 12, 20)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Put, 7200m, new DateTime(2024, 12, 20)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Put, 8800m, new DateTime(2025, 12, 19)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Put, 4400m, new DateTime(2025, 12, 19)),
                Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.European, OptionRight.Put, 4900m, new DateTime(2024, 12, 20)),
            };

            var leanSymbolsFromFos = mapper.GetLeanSymbolsFromFactSetFosSymbols(fosSymbols, SecurityType.IndexOption).ToList();

            Assert.That(leanSymbolsFromFos, Is.EquivalentTo(expectedLeanSymbols));
        }

        private FactSetApi GetApi(FactSetSymbolMapper symbolMapper)
        {
            return new FactSetApi(GetFactSetAuthConfig(), symbolMapper);
        }

        private TestableFactSetApi GetTestableApi(FactSetSymbolMapper symbolMapper, int batchSize)
        {
            return new TestableFactSetApi(GetFactSetAuthConfig(), symbolMapper, batchSize);
        }

        private FactSet.SDK.Utils.Authentication.Configuration GetFactSetAuthConfig()
        {
            var factSetAuthConfigurationStr = Config.Get("factset-auth-config");
            return JsonConvert.DeserializeObject<FactSet.SDK.Utils.Authentication.Configuration>(factSetAuthConfigurationStr);
        }

        private class TestableFactSetSymbolMapper : FactSetSymbolMapper
        {
            public new void SetApi(FactSetApi api)
            {
                base.SetApi(api);
            }
        }

        private class TestableFactSetApi : FactSetApi
        {
            public TestableFactSetApi(FactSet.SDK.Utils.Authentication.Configuration authConfig, FactSetSymbolMapper symbolMapper,
                int? batchSize = null)
                : base(authConfig, symbolMapper)
            {
                if (batchSize.HasValue)
                {
                    BatchSize = batchSize.Value;
                }
            }
        }
    }
}
