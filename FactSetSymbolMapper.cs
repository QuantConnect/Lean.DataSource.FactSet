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
using QuantConnect.Brokerages;

namespace QuantConnect.Lean.DataSource.FactSet
{
    /// <summary>
    /// </summary>
    public class FactSetSymbolMapper : ISymbolMapper
    {
        /// <summary>
        /// Get the FactSet symbol for a given Lean symbol
        /// </summary>
        /// <param name="symbol">The Lean symbol</param>
        /// <returns>The FactSet symbol</returns>
        public string GetBrokerageSymbol(Symbol symbol)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Get the Lean symbol for a given FactSet symbol
        /// </summary>
        /// <param name="brokerageSymbol">The FactSet symbol</param>
        /// <param name="securityType">The security type</param>
        /// <param name="market">The market</param>
        /// <param name="expirationDate">The expiration date</param>
        /// <param name="strike">The strike price</param>
        /// <param name="optionRight">The option right</param>
        /// <returns>The Lean symbol</returns>
        public Symbol GetLeanSymbol(string brokerageSymbol, SecurityType securityType, string market, DateTime expirationDate = default,
            decimal strike = 0, OptionRight optionRight = OptionRight.Call)
        {
            throw new NotImplementedException();
        }
    }
}