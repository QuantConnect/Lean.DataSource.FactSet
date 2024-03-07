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

using QuantConnect.Interfaces;
using System.Collections.Generic;
using System;
using System.Linq;
using QuantConnect.Logging;

namespace QuantConnect.Lean.DataSource.FactSet
{
    /// <summary>
    /// FactSet implementation of <see cref="IOptionChainProvider"/>
    /// </summary>
    public class FactSetOptionChainProvider : IOptionChainProvider
    {
        private static SecurityType[] _supportedSecurityTypes =
        {
            SecurityType.Equity,
            SecurityType.Index,
            SecurityType.Option,
            SecurityType.IndexOption
        };

        private FactSetApi _factSetApi;

        private bool _unsupportedSecurityTypeWarningSent;

        /// <summary>
        /// Creates a new instance of the <see cref="FactSetOptionChainProvider"/> class
        /// </summary>
        public FactSetOptionChainProvider(FactSetApi factSetApi)
        {
            _factSetApi = factSetApi;
        }

        /// <summary>
        /// Gets the list of option contracts for a given underlying symbol from Polygon's REST API
        /// </summary>
        /// <param name="symbol">The option or the underlying symbol to get the option chain for.
        /// Providing the option allows targeting an option ticker different than the default e.g. SPXW</param>
        /// <param name="date">The date for which to request the option chain (only used in backtesting)</param>
        /// <returns>The list of option contracts</returns>
        public IEnumerable<Symbol> GetOptionContractList(Symbol symbol, DateTime date)
        {
            if (!_supportedSecurityTypes.Contains(symbol.SecurityType))
            {
                if (!_unsupportedSecurityTypeWarningSent)
                {
                    Log.Trace($"FactSetOptionChainProvider.GetOptionContractList(): Unsupported security type {symbol.SecurityType}");
                    _unsupportedSecurityTypeWarningSent = true;
                }

                return Enumerable.Empty<Symbol>();
            }

            var underlying = symbol.SecurityType.IsOption() ? symbol.Underlying : symbol;

            return _factSetApi.GetOptionsChain(underlying, date);
        }
    }
}