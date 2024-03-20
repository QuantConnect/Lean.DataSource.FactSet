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

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class FactSetSymbolMapperTests
    {
        private static IEnumerable<Tuple<string, Symbol>> SymbolConvertionTestCases()
        {
            // Equity
            yield return new Tuple<string, Symbol>("SPY", Symbols.SPY);

            // Index
            yield return new Tuple<string, Symbol>("SPX", Symbols.SPX);

            // Options
            var msft = Symbol.Create("MSFT", SecurityType.Equity, Market.USA);
            var msftOption = Symbol.CreateOption(msft, Market.USA, OptionStyle.American, OptionRight.Call, 26m, new DateTime(2009, 11, 21));

            yield return new Tuple<string, Symbol>("MSFT#091121C00026000", msftOption);
            // Index Options

            var spxOption = Symbol.CreateOption(Symbols.SPX, Market.USA, OptionStyle.American, OptionRight.Put, 5210m, new DateTime(2024, 03, 28));
            yield return new Tuple<string, Symbol>("SPX#240328P05210000", spxOption);
        }

        private static IEnumerable<TestCaseData> FactSetToLeanSymbolConversionTestCases()
        {
            return SymbolConvertionTestCases().Select(x => new TestCaseData(x.Item1, x.Item2));
        }

        [TestCaseSource(nameof(FactSetToLeanSymbolConversionTestCases))]
        public void ConvertsFactSetOCC21SymbolToLeanSymbol(string factSetOcc21Symbol, Symbol expectedLeanSymbol)
        {
            var mapper = new FactSetSymbolMapper();
            var leanSymbol = mapper.GetLeanSymbol(factSetOcc21Symbol, expectedLeanSymbol.SecurityType, Market.USA);
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
    }
}
