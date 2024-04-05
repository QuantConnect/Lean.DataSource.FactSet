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
using System.Linq;
using System.Threading.Tasks;
using QuantConnect.Data;
using QuantConnect.Lean.DataSource.FactSet;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// Helper class to download, process and store FactSet data
    /// </summary>
    public class FactSetDataProcessor : IDisposable
    {
        private readonly string _destinationFolder;
        private readonly string _rawDataFolder;
        private readonly List<string> _tickerWhitelist;
        private readonly FactSet.SDK.Utils.Authentication.Configuration _factSetAuthConfig;
        private readonly FactSetDataProcessingDataDownloader _downloader;

        private List<Symbol> _symbols;
        private Resolution _resolution;
        private DateTime _startDate;
        private DateTime _endDate;

        /// <summary>
        /// Creates a new instance of the <see cref="FactSetDataProcessor"/> class.
        /// </summary>
        /// <param name="factSetAuthConfig">The FactSet authentication configuration</param>
        /// <param name="symbols">The symbols to download data for</param>
        /// <param name="resolution">The resolution of the data to download</param>
        /// <param name="startDate">The start date of the data to download</param>
        /// <param name="endDate">The end date of the data to download</param>
        /// <param name="destinationFolder">The destination folder to save the data</param>
        /// <param name="rawDataFolder">The raw data folder</param>
        /// <param name="tickerWhitelist">A list of supported tickers</param>
        public FactSetDataProcessor(FactSet.SDK.Utils.Authentication.Configuration factSetAuthConfig, List<Symbol> symbols, Resolution resolution,
            DateTime startDate, DateTime endDate, string destinationFolder, string rawDataFolder, List<string> tickerWhitelist = null)
        {
            _factSetAuthConfig = factSetAuthConfig;
            _symbols = symbols;
            _resolution = resolution;
            _startDate = startDate;
            _endDate = endDate;
            _destinationFolder = destinationFolder;
            _rawDataFolder = rawDataFolder;
            _tickerWhitelist = tickerWhitelist ?? new List<string>();

            if (_symbols.Any(symbol => !symbol.SecurityType.IsOption() || !symbol.IsCanonical() || !_tickerWhitelist.Contains(symbol.Underlying.Value)))
            {
                throw new ArgumentException("The symbols must be canonical option symbols and the underlying must be in the whitelist.");
            }

            _downloader = new FactSetDataProcessingDataDownloader(_factSetAuthConfig, _rawDataFolder);
        }

        /// <summary>
        /// Disposes of the resources
        /// </summary>
        public void Dispose()
        {
            _downloader.DisposeSafely();
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run()
        {
            var stopwatch = Stopwatch.StartNew();

            // Let's get the options ourselves so the data downloader doesn't have to do it for each tick type
            var symbolsStr = string.Join(", ", _symbols.Select(symbol => symbol.Value));
            Log.Trace($"FactSetDataProcessor.Run(): Fetching options for {symbolsStr}.");
            var options = _symbols.Select(symbol => _downloader.GetOptionChains(symbol, _startDate, _startDate)).SelectMany(x => x).ToList();

            Log.Trace($"FactSetDataProcessor.Run(): Found {options.Count} options.");
            Log.Trace($"FactSetDataProcessor.Run(): Start downloading/processing {symbolsStr} {_resolution} data.");

            var tickTypes = new[] { TickType.Trade, TickType.Quote, TickType.OpenInterest };
            var source = options.Select(option => tickTypes.Select(tickType => (option, tickType))).SelectMany(x => x);

            var result = Parallel.ForEach(source, new ParallelOptions() { MaxDegreeOfParallelism = 16 }, (t, loopState) =>
                {
                    var option = t.option;
                    var tickType = t.tickType;

                    var data = _downloader.Get(new DataDownloaderGetParameters(option, _resolution, _startDate, _endDate, tickType));
                    if (data == null)
                    {
                        Log.Trace($"FactSetDataProcessor.Run(): No {tickType} data found for {symbolsStr}.");
                    loopState.Stop();
                        return;
                    }

                    var tradesWriter = new LeanDataWriter(_resolution, option, _destinationFolder, tickType);
                    tradesWriter.Write(data);
                });

            if (!result.IsCompleted)
            {
                Log.Error($"FactSetDataProcessor.Run(): Failed to download/processing {symbolsStr} {_resolution} data.");
                return false;
            }

            Log.Trace($"FactSetDataProcessor.Run(): Finished in {stopwatch.Elapsed.ToStringInvariant(null)}");
            return true;
        }
    }
}