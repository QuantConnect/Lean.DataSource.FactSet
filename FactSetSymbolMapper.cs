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
using QuantConnect.Brokerages;

namespace QuantConnect.Lean.DataSource.FactSet
{
    /// <summary>
    /// Provides basic mapping between Lean symbols and FactSet symbols.
    /// Reference: https://assets.ctfassets.net/lmz2w5z92b9u/2yNjkkO0Q1ZMsBpGQ6KfHn/62008ef87b8c154e99ecd4186e86851a/FactSetTickHistory_DataModel_V1.0E.pdf Section 3.3 "Options Identifiers"
    /// </summary>
    public class FactSetSymbolMapper : ISymbolMapper
    {
        private readonly Dictionary<string, Symbol> _occ21ToLeanSymbolsCache = new();
        private readonly Dictionary<Symbol, string> _leanToOcc21SymbolsCache = new();
        private readonly object _occ21SymbolsLock = new();

        private readonly Dictionary<Symbol, string> _leanToFactSetFosSymbolCache = new();
        private readonly Dictionary<string, Symbol> _factSetFosToLeanSymbolCache = new();
        private readonly object _fosSymbolsLock = new();

        private FactSetApi? _api;

        /// <summary>
        /// Set the api to use for symbol mapping
        /// </summary>
        /// <remarks>Intended for internal use</remarks>
        internal protected void SetApi(FactSetApi api)
        {
            _api = api;
        }

        /// <summary>
        /// Get the FactSet symbol in their OCC21 format for a given Lean symbol
        /// </summary>
        /// <param name="symbol">The Lean symbol</param>
        /// <returns>The FactSet symbol</returns>
        public string GetBrokerageSymbol(Symbol symbol)
        {
            return GetFactSetOCC21Symbol(symbol);
        }

        /// <summary>
        /// Get the Lean symbol for a given FactSet symbol (in their OCC21 format)
        /// </summary>
        /// <param name="brokerageSymbol">The FactSet OCC21 symbol</param>
        /// <param name="securityType">The security type</param>
        /// <param name="market">The market</param>
        /// <param name="expirationDate">The expiration date</param>
        /// <param name="strike">The strike price</param>
        /// <param name="optionRight">The option right</param>
        /// <returns>The Lean symbol</returns>
        public Symbol GetLeanSymbol(string brokerageSymbol, SecurityType securityType, string market, DateTime expirationDate = default,
            decimal strike = 0, OptionRight optionRight = OptionRight.Call)
        {
            return ParseFactSetOCC21Symbol(brokerageSymbol, securityType, market);
        }

        /// <summary>
        /// Get the FactSet symbol in their OCC21 format for a given Lean symbol
        /// </summary>
        /// <param name="symbol">The Lean symbol</param>
        /// <returns>The FactSet symbol</returns>
        public string GetFactSetOCC21Symbol(Symbol symbol)
        {
            if (symbol == null || string.IsNullOrWhiteSpace(symbol.Value))
            {
                throw new ArgumentException($"Invalid symbol: {(symbol == null ? "null" : symbol.ToString())}", nameof(symbol));
            }

            lock (_occ21SymbolsLock)
            {
                if (!_leanToOcc21SymbolsCache.TryGetValue(symbol, out var brokerageSymbol))
                {
                    switch (symbol.SecurityType)
                    {
                        case SecurityType.Equity:
                        case SecurityType.Index:
                            brokerageSymbol = symbol.Value;
                            break;

                        case SecurityType.Option:
                        case SecurityType.IndexOption:
                            brokerageSymbol = $"{symbol.Value.Substring(0, 6).TrimEnd()}#{symbol.Value.Substring(6)}";
                            break;

                        default:
                            throw new ArgumentException($"Unsupported security type: {symbol.SecurityType}", nameof(symbol));
                    }

                    _leanToOcc21SymbolsCache[symbol] = brokerageSymbol;
                    _occ21ToLeanSymbolsCache[brokerageSymbol] = symbol;
                }

                return brokerageSymbol;
            }
        }

        /// <summary>
        /// Parse the FactSet symbol in their OCC21 format to a Lean symbol
        /// </summary>
        /// <param name="occ21Symbol">The FactSet symbol in OCC21 format</param>
        /// <param name="securityType">The symbol security type</param>
        /// <param name="market">The market</param>
        /// <param name="optionStyle">The option style</param>
        /// <returns>The corresponding Lean symbol</returns>
        public Symbol ParseFactSetOCC21Symbol(string occ21Symbol, SecurityType securityType, string market = Market.USA,
            OptionStyle optionStyle = OptionStyle.American)
        {
            if (string.IsNullOrEmpty(occ21Symbol))
            {
                throw new ArgumentException($"Invalid OCC21 symbol: {occ21Symbol}", nameof(occ21Symbol));
            }

            lock (_occ21SymbolsLock)
            {
                if (!_occ21ToLeanSymbolsCache.TryGetValue(occ21Symbol, out var symbol))
                {
                    switch (securityType)
                    {
                        case SecurityType.Equity:
                        case SecurityType.Index:
                            symbol = Symbol.Create(occ21Symbol, securityType, market);
                            break;

                        case SecurityType.Option:
                        case SecurityType.IndexOption:
                            var parts = occ21Symbol.Split('#');
                            if (parts.Length != 2)
                            {
                                throw new ArgumentException($"Invalid OCC21 symbol: {occ21Symbol}", nameof(occ21Symbol));
                            }

                            // FactSet symbol might end with "-{Exchange OSI}" (e.g. -US) but it currently only supports US
                            var osiTicker = parts[0].PadRight(6, ' ') + parts[1].Split("-")[0];
                            symbol = SymbolRepresentation.ParseOptionTickerOSI(osiTicker, securityType, optionStyle, market);
                            break;

                        default:
                            throw new ArgumentException($"Unsupported security type: {securityType}", nameof(occ21Symbol));
                    }

                    _occ21ToLeanSymbolsCache[occ21Symbol] = symbol;
                    _leanToOcc21SymbolsCache[symbol] = occ21Symbol;
                }

                return symbol;
            }
        }

        /// <summary>
        /// Gets the FactSet FOS (FactSet Option Symbology) representation of the given Lean symbol
        /// </summary>
        /// <param name="leanSymbol">The lean symbol</param>
        /// <returns>The corresponding FOS symbol</returns>
        public string? GetFactSetFosSymbol(Symbol leanSymbol)
        {
            if (_api == null)
            {
                throw new InvalidOperationException($"FactSetSymbolMapper.GetFactSetFosSymbol(): " +
                    $"Could not fetch FactSet FOS symbol for {leanSymbol}. The API instance is not set.");
            }

            if (leanSymbol == null)
            {
                throw new ArgumentNullException(nameof(leanSymbol), "Invalid lean symbol");
            }

            if (leanSymbol.SecurityType != SecurityType.Option && leanSymbol.SecurityType != SecurityType.IndexOption)
            {
                throw new ArgumentException($"Invalid symbol security type {leanSymbol.SecurityType}", nameof(leanSymbol));
            }

            lock (_fosSymbolsLock)
            {
                if (!_leanToFactSetFosSymbolCache.TryGetValue(leanSymbol, out var fosSymbol))
                {
                    var optionDetails = _api.GetOptionDetails(leanSymbol);
                    if (optionDetails == null)
                    {
                        return null;
                    }

                    _leanToFactSetFosSymbolCache[leanSymbol] = fosSymbol = optionDetails.FsymId;
                    _factSetFosToLeanSymbolCache[fosSymbol] = leanSymbol;
                }

                return fosSymbol;
            }
        }

        /// <summary>
        /// Gets the Lean symbol corresponding to the given FactSet FOS (FactSet Option Symbology) symbol
        /// </summary>
        /// <param name="fosSymbol">The FactSet FOS symbol</param>
        /// <param name="securityType">The security type</param>
        /// <returns>The corresponding Lean symbol</returns>
        public Symbol? GetLeanSymbolFromFactSetFosSymbol(string fosSymbol, SecurityType securityType)
        {
            if (_api == null)
            {
                throw new InvalidOperationException($"FactSetSymbolMapper.GetLeanSymbolFromFactSetFosSymbol(): " +
                    $"Could not fetch Lean symbol for {fosSymbol}. The API instance is not set.");
            }

            if (string.IsNullOrEmpty(fosSymbol))
            {
                throw new ArgumentNullException(nameof(fosSymbol), "Invalid FOS symbol");
            }

            if (securityType != SecurityType.Option && securityType != SecurityType.IndexOption)
            {
                throw new ArgumentException($"Invalid symbol security type {securityType}", nameof(securityType));
            }

            lock (_fosSymbolsLock)
            {
                if (!_factSetFosToLeanSymbolCache.TryGetValue(fosSymbol, out var leanSymbol))
                {
                    var optionDetails = _api.GetOptionDetails(fosSymbol);
                    if (optionDetails == null)
                    {
                        throw new InvalidOperationException($"Could not fetch option details for {fosSymbol}");
                    }

                    // Careful here. It is documented that 0=American and 1=European, but the API returns 2 for SPX, for example
                    var optionStyle = !optionDetails.Style.HasValue || optionDetails.Style.Value == 0 ? OptionStyle.American : OptionStyle.European;

                    _factSetFosToLeanSymbolCache[fosSymbol] = leanSymbol = ParseFactSetOCC21Symbol(optionDetails.Occ21Symbol, securityType,
                        optionStyle: optionStyle);
                    _leanToFactSetFosSymbolCache[leanSymbol] = fosSymbol;
                }

                return leanSymbol;
            }
        }

        /// <summary>
        /// Gets the Lean symbols corresponding to the given FactSet FOS (FactSet Option Symbology) symbols
        /// </summary>
        /// <param name="fosSymbols">The FactSet FOS symbols</param>
        /// <param name="securityType">The security type</param>
        /// <returns>The corresponding Lean symbols</returns>
        public IEnumerable<Symbol>? GetLeanSymbolsFromFactSetFosSymbols(List<string> fosSymbols, SecurityType securityType)
        {
            if (_api == null)
            {
                throw new InvalidOperationException($"FactSetSymbolMapper.GetLeanSymbolsFromFactSetFosSymbols(): " +
                    $"Could not fetch Lean symbol for the given FOS symbols. The API instance is not set.");
            }

            if (fosSymbols == null)
            {
                throw new ArgumentNullException(nameof(fosSymbols), "Invalid FOS symbol");
            }

            if (securityType != SecurityType.Option && securityType != SecurityType.IndexOption)
            {
                throw new ArgumentException($"Invalid symbol security type {securityType}", nameof(securityType));
            }

            var detailsList = _api.GetOptionDetails(fosSymbols);

            if (detailsList == null)
            {
                throw new InvalidOperationException("Could not fetch option details for the given FOS symbols");
            }

            foreach (var details in detailsList)
            {
                if (details == null || details.FsymId == null)
                {
                    continue;
                }

                var leanSymbol = ParseFactSetOCC21Symbol(details.Occ21Symbol,
                    securityType,
                    optionStyle: !details.Style.HasValue || details.Style.Value == 0 ? OptionStyle.American : OptionStyle.European);

                // Add the mapping to the FOS symbol cache
                lock (_fosSymbolsLock)
                {
                    _factSetFosToLeanSymbolCache[details.FsymId] = leanSymbol;
                    _leanToFactSetFosSymbolCache[leanSymbol] = details.FsymId;
                }

                yield return leanSymbol;
            }
        }
    }
}