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
*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using QuantConnect.Data;
using QuantConnect.Logging;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// </summary>
    public class FactSetDataProcessor
    {
        public const string VendorName = "FactSet";
        public const string VendorDataName = "FactSet";

        private readonly List<string> _tickerWhitelist;
        private readonly string _destinationFolder;
        private readonly string _rawDataFolder;

        private readonly FactSet.SDK.Utils.Authentication.Configuration _factSetAuthConfig;
        private readonly FactSetDataProcessingDataDownloader _downloader;

        private Symbol _symbol;
        private Resolution _resolution;
        private DateTime _startDate;
        private DateTime _endDate;

        /// <summary>
        /// Creates a new instance of the <see cref="FactSetDataProcessor"/> class.
        /// </summary>
        /// <param name="factSetAuthConfig">The FactSet authentication configuration</param>
        /// <param name="symbol">The symbol to download data for</param>
        /// <param name="resolution">The resolution of the data to download</param>
        /// <param name="startDate">The start date of the data to download</param>
        /// <param name="endDate">The end date of the data to download</param>
        /// <param name="destinationFolder">The destination folder to save the data</param>
        /// <param name="rawDataFolder">The raw data folder</param>
        /// <param name="tickerWhitelist">A list of supported tickers</param>
        public FactSetDataProcessor(FactSet.SDK.Utils.Authentication.Configuration factSetAuthConfig, Symbol symbol, Resolution resolution,
            DateTime startDate, DateTime endDate, string destinationFolder, string rawDataFolder, List<string> tickerWhitelist = null)
        {
            _factSetAuthConfig = factSetAuthConfig;
            _symbol = symbol;
            _resolution = resolution;
            _startDate = startDate;
            _endDate = endDate;
            _destinationFolder = destinationFolder;
            _rawDataFolder = rawDataFolder;
            _tickerWhitelist = tickerWhitelist ?? new List<string>();

            if (_symbol.SecurityType != SecurityType.IndexOption || !_symbol.IsCanonical())
            {
                throw new ArgumentException($"Invalid symbol {symbol}. Only canonical {SecurityType.IndexOption} are supported.");
            }

            if (!_tickerWhitelist.Contains(symbol.ID.Symbol))
            {
                throw new ArgumentException($"Symbol {symbol} is not currently supported.");
            }

            if (_resolution != Resolution.Daily)
            {
                throw new ArgumentException($"Unsupported resolution {_resolution}. Only {Resolution.Daily} is currently supported.");
            }

            _downloader = new FactSetDataProcessingDataDownloader(_factSetAuthConfig, _rawDataFolder);
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run()
        {
            var stopwatch = Stopwatch.StartNew();

            Log.Trace($"FactSetDataProcessor.Run(): Start downloading/processing {_symbol} {_resolution} data.");

            // TODO: Since data is daily, should we normalize the start and end dates to the beginning and end of their years?

            var tradesTask = Task.Run(() =>
            {
                var trades = _downloader.Get(new DataDownloaderGetParameters(_symbol, _resolution, _startDate, _endDate, TickType.Trade));
                if (trades == null)
                {
                    Log.Trace($"FactSetDataProcessor.Run(): No trade data found for {_symbol}.");
                    return false;
                }

                var tradesWriter = new LeanDataWriter(_resolution, _symbol, _destinationFolder, TickType.Trade);
                tradesWriter.Write(trades);
                return true;
            });

            var openInterestTask = Task.Run(() =>
            {
                var openInterest = _downloader.Get(new DataDownloaderGetParameters(_symbol, _resolution, _startDate, _endDate, TickType.OpenInterest));
                if (openInterest == null)
                {
                    Log.Trace($"FactSetDataProcessor.Run(): No open interest data found for {_symbol}.");
                    return false;
                }

                var openInterestWriter = new LeanDataWriter(_resolution, _symbol, _destinationFolder, TickType.OpenInterest);
                openInterestWriter.Write(openInterest);
                return true;
            });

            Task.WaitAll(tradesTask, openInterestTask);

            if (!tradesTask.Result && !openInterestTask.Result)
            {
                Log.Error($"FactSetDataProcessor.Run(): Failed to download/processing {_symbol} {_resolution} data.");
                return false;
            }

            Log.Trace($"FactSetDataProcessor.Run(): Finished in {stopwatch.Elapsed.ToStringInvariant(null)}");

            return true;
        }
    }
}