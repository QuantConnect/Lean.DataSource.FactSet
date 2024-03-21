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
        private readonly object _lock = new();

        private FactSetApi _api;
        private readonly Dictionary<Symbol, string> _leanToFactSetFosSymbolCache = new();

        internal void SetApi(FactSetApi api)
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
                throw new ArgumentException(nameof(symbol), $"Invalid symbol: {(symbol == null ? "null" : symbol.ToString())}");
            }

            lock (_lock)
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
                            throw new ArgumentException(nameof(symbol), $"Unsupported security type: {symbol.SecurityType}");
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
        /// <returns>The corresponding Lean symbol</returns>
        public Symbol ParseFactSetOCC21Symbol(string occ21Symbol, SecurityType securityType, string market = Market.USA)
        {
            if (string.IsNullOrEmpty(occ21Symbol))
            {
                throw new ArgumentException(nameof(occ21Symbol), $"Invalid OCC21 symbol: {occ21Symbol}");
            }

            lock (_lock)
            {
                if (!_occ21ToLeanSymbolsCache.TryGetValue(occ21Symbol, out var symbol))
                {
                    switch (securityType)
                    {
                        case SecurityType.Equity:
                        case SecurityType.Index:
                            symbol = Symbol.Create(occ21Symbol, securityType, Market.USA);
                            break;

                        case SecurityType.Option:
                        case SecurityType.IndexOption:
                            var parts = occ21Symbol.Split('#');
                            if (parts.Length != 2)
                            {
                                throw new ArgumentException(nameof(occ21Symbol), $"Invalid OCC21 symbol: {occ21Symbol}");
                            }

                            // FactSet symbol might end with "-{Exchange OSI}" (e.g. -US) but it currently only supports US
                            var osiTicker = parts[0].PadRight(6, ' ') + parts[1].Split("-")[0];
                            symbol = SymbolRepresentation.ParseOptionTickerOSI(osiTicker, securityType, Market.USA);
                            break;

                        default:
                            throw new ArgumentException(nameof(occ21Symbol), $"Unsupported security type: {securityType}");
                    }

                    _occ21ToLeanSymbolsCache[occ21Symbol] = symbol;
                    _leanToOcc21SymbolsCache[symbol] = occ21Symbol;
                }

                return symbol;
            }
        }

        /// <summary>
        /// Gets the FactSet FOS (FactSet Option Symbology) representation of the given Lean Symbol
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
                throw new ArgumentException(nameof(leanSymbol), $"Invalid symbol security type {leanSymbol.SecurityType}");
            }

            lock (_leanToFactSetFosSymbolCache)
            {
                if (!_leanToFactSetFosSymbolCache.TryGetValue(leanSymbol, out var fosSymbol))
                {
                    var optionDetails = _api.GetOptionDetails(leanSymbol);
                    if (optionDetails == null)
                    {
                        return null;
                    }

                    _leanToFactSetFosSymbolCache[leanSymbol] = fosSymbol = optionDetails.FsymId;
                }

                return fosSymbol;
            }
        }
    }
}