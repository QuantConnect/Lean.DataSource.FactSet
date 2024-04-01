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
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Linq;
using System.Net;
using Newtonsoft.Json;
using FactSet.SDK.Utils.Authentication;
using FactSet.SDK.FactSetOptions.Api;
using FactSet.SDK.FactSetOptions.Model;
using FactSet.SDK.FactSetOptions.Client;
using QuantConnect.Logging;
using QuantConnect.Data.Market;
using FactSetOptionsClientConfiguration = FactSet.SDK.FactSetOptions.Client.Configuration;
using FactSetAuthenticationConfiguration = FactSet.SDK.Utils.Authentication.Configuration;
using QuantConnect.Util;
using System.IO.Compression;

namespace QuantConnect.Lean.DataSource.FactSet
{
    /// <summary>
    /// Wrapper around FactSet APIs
    /// </summary>
    public class FactSetApi : IDisposable
    {
        private static KeyStringSynchronizer _keySynchronizer = new();

        private FactSetAuthenticationConfiguration _factSetAuthConfiguration;
        private FactSetOptionsClientConfiguration _factSetOptionsClientConfig;
        private PricesVolumeApi _pricesVolumeApi;
        private OptionChainsScreeningApi _optionChainsScreeningApi;
        private ReferenceApi _optionsReferenceApi;

        private FactSetSymbolMapper _symbolMapper;

        private RateGate _rateLimiter;

        private string? _rawDataFolder;

        private bool ShouldStoreRawData => !string.IsNullOrEmpty(_rawDataFolder);

        /// <summary>
        /// The max batch size for requests. Made a protected property for testing purposes
        /// </summary>
        protected int BatchSize { get; set; } = 100;

        /// <summary>
        /// Creates a new instance of the <see cref="FactSetApi"/> class
        /// </summary>
        /// <param name="factSetAuthConfiguration">The FactSet authentication configuration for the API</param>
        /// <param name="symbolMapper">The symbol mapper</param>
        /// <param name="rawDataFolder">The raw data folder</param>
        public FactSetApi(FactSetAuthenticationConfiguration factSetAuthConfiguration, FactSetSymbolMapper symbolMapper,
            string? rawDataFolder = null)
        {
            _factSetAuthConfiguration = factSetAuthConfiguration;
            _symbolMapper = symbolMapper;
            _rawDataFolder = rawDataFolder;

            var factSetConfidentialClient = ConfidentialClient.CreateAsync(_factSetAuthConfiguration).SynchronouslyAwaitTaskResult();

            _factSetOptionsClientConfig = new FactSetOptionsClientConfiguration();
            _factSetOptionsClientConfig.OAuth2Client = factSetConfidentialClient;

            _pricesVolumeApi = new PricesVolumeApi(_factSetOptionsClientConfig);
            _optionChainsScreeningApi = new OptionChainsScreeningApi(_factSetOptionsClientConfig);
            _optionsReferenceApi = new ReferenceApi(_factSetOptionsClientConfig);

            _rateLimiter = new(40, TimeSpan.FromMinutes(1));
        }

        /// <summary>
        /// Disposes of the FactSet API resources
        /// </summary>
        public void Dispose()
        {
            _rateLimiter.DisposeSafely();
        }

        /// <summary>
        /// Gets the option chain for the specified symbol and date
        /// </summary>
        /// <param name="underlying">The underlying symbol</param>
        /// <param name="date">The date</param>
        /// <param name="canonical">The canonical option. This is useful for index options, for instance, to filter out weeklys</param>
        /// <returns>The option contract list</returns>
        public IEnumerable<Symbol> GetOptionsChain(Symbol underlying, DateTime date, Symbol? canonical = null)
        {
            if (underlying.SecurityType != SecurityType.Equity && underlying.SecurityType != SecurityType.Index)
            {
                throw new ArgumentException("Underlying security type must be either Equity or Index", nameof(underlying));
            }

            if (canonical != null && !canonical.IsCanonical() && canonical.Underlying != underlying)
            {
                throw new ArgumentException("Canonical symbol must be a canonical option symbol for the given underlying", nameof(canonical));
            }

            // While the FactSet options API is in beta, we will use the /option-screening endpoint to get the option chain
            // instead of the /chains endpoint to reduce the chances of timeouts (reference: https://developer.factset.com/api-catalog/factset-options-api)
            // With the /option-screening endpoint we can split the request into two parts: one for calls and one for puts.

            // 1=Equity, 2=Index Options
            var optionType = underlying.SecurityType == SecurityType.Equity ? "1" : "2";
            var tasks = new[] { OptionRight.Call, OptionRight.Put }
                .Select(optionRight => Task.Run(() =>
                {
                    // 0=Call, 1=Put
                    var factSetOptionRight = optionRight == OptionRight.Call ? "0" : "1";
                    var request = new OptionScreeningRequest(ExchangeScreeningId.ALLUSAOPTS,
                        OptionScreeningRequest.ConditionOneEnum.UNDERLYINGSECURITYE, underlying.Value,
                        OptionScreeningRequest.ConditionTwoEnum.OPTIONTYPEE, optionType,
                        OptionScreeningRequest.ConditionThreeEnum.CALLORPUTE, factSetOptionRight,
                        date: FactSetUtils.ParseDate(date));

                    CheckRequestRate();
                    return _optionChainsScreeningApi.GetOptionsScreeningForList(request);
                }))
                .ToArray();

            var requestFunc = () =>
            {
                return tasks.SelectMany(task => task.Result.Data).Select(optionScreening => optionScreening.OptionId);
            };

            if (!TryRequest(requestFunc, out var optionsFosIds, out var exception))
            {
                Log.Error(exception, $"FactSetApi.GetOptionsChain(): Error requesting option chain for {underlying}: {exception.Message}");
                return Enumerable.Empty<Symbol>();
            }

            var optionSecurityType = Symbol.GetOptionTypeFromUnderlying(underlying.SecurityType);
            var leanSymbols = _symbolMapper.GetLeanSymbolsFromFactSetFosSymbols(optionsFosIds.ToList(), optionSecurityType);

            if (leanSymbols == null)
            {
                Log.Error($"FactSetApi.GetOptionsChain(): Error mapping FactSet FOS symbols to Lean symbols for {underlying}");
                return Enumerable.Empty<Symbol>();
            }

            if (canonical != null)
            {
                return leanSymbols.Where(symbol => symbol.Canonical == canonical);
            }

            return leanSymbols;
        }

        /// <summary>
        /// Returns basic reference details for the option
        /// </summary>
        /// <param name="option">The option which details are requested</param>
        /// <returns>The option reference details. Null if the request fails.</returns>
        public OptionsReferences? GetOptionDetails(Symbol option)
        {
            var factSetSymbol = _symbolMapper.GetFactSetOCC21Symbol(option);
            return GetOptionDetails(factSetSymbol);
        }

        /// <summary>
        /// Returns basic reference details for the option
        /// </summary>
        /// <param name="factSetSymbol">The option which details are requested. It can be either FOS or OCC21.</param>
        /// <returns>The option reference details. Null if the request fails.</returns>
        public OptionsReferences? GetOptionDetails(string factSetSymbol)
        {
            return GetOptionDetails(new List<string>() { factSetSymbol })?.SingleOrDefault();
        }

        /// <summary>
        /// Returns basic reference details for the option
        /// </summary>
        /// <param name="factSetSymbols">The options which details are requested. They can be either FOS or OCC21.</param>
        /// <returns>The options reference details. Null if the request fails.</returns>
        public IEnumerable<OptionsReferences>? GetOptionDetails(List<string> factSetSymbols)
        {
            // Let's get the details in batches to avoid requests timing out
            var batchCount = (int)Math.Ceiling((double)factSetSymbols.Count / BatchSize);
            var detailsBatches = Enumerable.Repeat(default(IEnumerable<OptionsReferences>), batchCount).ToList();
            CancellationTokenSource cts = new();

            try
            {
                Parallel.ForEach(Enumerable.Range(0, batchCount),
                    new ParallelOptions()
                    {
                        MaxDegreeOfParallelism = 10,
                        CancellationToken = cts.Token,
                    },
                    (batchIndex) =>
                    {
                        var batch = factSetSymbols.Skip(batchIndex * BatchSize).Take(BatchSize).ToList();
                        var details = GetOptionDetailsImpl(batch);

                        if (details == null)
                        {
                            cts.Cancel();
                            return;
                        }

                        detailsBatches[batchIndex] = details;
                    });
            }
            catch (OperationCanceledException)
            {
                throw new InvalidOperationException("Could not fetch option details for the given options FOS symbols.");
            }

            foreach (var batch in detailsBatches)
            {
                foreach (var symbol in batch)
                {
                    yield return symbol;
                }
            }
        }

        private IEnumerable<OptionsReferences>? GetOptionDetailsImpl(List<string> factSetSymbols)
        {
            var request = new OptionsReferencesRequest(factSetSymbols);
            var requestFunc = () =>
            {
                CheckRequestRate();
                return _optionsReferenceApi.GetOptionsReferencesForList(request).Data;
            };

            if (!TryRequest(requestFunc, out var result, out var exception))
            {
                Log.Error(exception, $"FactSetApi.GetOptionDetails(): Error requesting option details for given FOS symbols: {exception.Message}");
                return null;
            }

            return result;
        }

        /// <summary>
        /// Gets the trades for the specified symbol and date range
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <param name="startTime">The start time</param>
        /// <param name="endTime">The end time</param>
        /// <returns>The list of trade bars for the specified symbol and date range</returns>
        public IEnumerable<TradeBar>? GetDailyOptionsTrades(Symbol symbol, DateTime startTime, DateTime endTime)
        {
            if (!TryGetFactSetFosSymbol(symbol, out var factSetSymbol))
            {
                return null;
            }

            var startDate = FactSetUtils.ParseDate(startTime.Date);
            var endDate = FactSetUtils.ParseDate(endTime.Date);
            var symbolList = new List<string>() { factSetSymbol };

            var getPricesTask = Task.Run(() =>
            {
                var getPrices = () =>
                {
                    CheckRequestRate();
                    var pricesRequest = new OptionsPricesRequest(symbolList, startDate, endDate, Frequency.D);
                    var pricesResponse = _pricesVolumeApi.GetOptionsPricesForList(pricesRequest);

                    return pricesResponse.Data;
                };

                if (!TryRequest(getPrices, out var prices, out var exception))
                {
                    Log.Error(exception, $"FactSetApi.GetDailyOptionsTrades(): Error requesting option prices for {symbol} " +
                        $"between {startTime} and {endTime}: {exception.Message}");
                    return null;
                }

                return prices;
            });

            var getVolumesTask = Task.Run(() =>
            {
                var getVolumes = () =>
                {
                    CheckRequestRate();
                    var volumeRequest = new OptionsVolumeRequest(symbolList, startDate, endDate, Frequency.D);
                    var volumeResponse = _pricesVolumeApi.GetOptionsVolumeForList(volumeRequest);

                    return volumeResponse.Data;
                };

                if (!TryRequest(getVolumes, out var volumes, out var exception))
                {
                    Log.Error(exception, $"FactSetApi.GetDailyOptionsTrades(): Error requesting option volumes for {symbol} " +
                        $"between {startTime} and {endTime}: {exception.Message}");
                    return null;
                }

                return volumes;
            });

            var prices = getPricesTask.Result;
            var volumes = getVolumesTask.Result;

            // When data is requested for an invalid symbol, FactSet returns a single record with all fields set to null, except for the request id
            if (prices.Count == 1 && prices[0].FsymId == null)
            {
                Log.Error($"FactSetApi.GetDailyOptionsTrades(): No data found for {symbol} between {startTime} and {endTime}");
                return null;
            }

            StoreRawDailyOptionPrices(symbol, prices);
            StoreRawDailyOptionVolumes(symbol, volumes);

            return GetDailyOptionTradeBars(symbol, prices, volumes).Where(bar => bar.EndTime >= startTime && bar.Time <= endTime);
        }

        private IEnumerable<TradeBar> GetDailyOptionTradeBars(Symbol symbol, List<OptionsPrices> prices, List<OptionsVolume> volumes)
        {
            // Prices and volumes should be guaranteed to have the same length, but just in case we do some checking
            var priceIndex = 0;
            var volumeIndex = 0;
            while (priceIndex < prices.Count && volumeIndex < volumes.Count)
            {
                var priceData = prices[priceIndex++];
                var volumeData = volumes[volumeIndex++];

                var priceDate = priceData.Date.GetValueOrDefault();
                var volumeDate = volumeData.Date.GetValueOrDefault();
                // Just for safety, shouldn't happen
                if (priceDate == default || volumeDate == default)
                {
                    continue;
                }

                if (priceDate != volumeDate)
                {
                    // We need to check which is first
                    if (volumeDate > priceDate)
                    {
                        // We will re-process the current volume data with the next price data
                        volumeIndex--;
                    }
                    else
                    {
                        // We will re-process the current price data with the next volume data
                        priceIndex--;
                    }

                    continue;
                }

                var volume = (decimal)volumeData.Volume.GetValueOrDefault();
                if (volume == 0)
                {
                    continue;
                }

                yield return new TradeBar(priceDate,
                    symbol,
                    (decimal)priceData.PriceOpen.GetValueOrDefault(),
                    (decimal)priceData.PriceHigh.GetValueOrDefault(),
                    (decimal)priceData.PriceLow.GetValueOrDefault(),
                    (decimal)priceData.Price.GetValueOrDefault(),
                    volume,
                    Time.OneDay);
            }
        }

        /// <summary>
        /// Gets the open interest data for the specified symbol and date range
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <param name="startTime">The start time</param>
        /// <param name="endTime">The end time</param>
        /// <returns>The list of open interest for the specified symbol and date range</returns>
        public IEnumerable<OpenInterest>? GetDailyOpenInterest(Symbol symbol, DateTime startTime, DateTime endTime)
        {
            if (!TryGetFactSetFosSymbol(symbol, out var factSetSymbol))
            {
                return null;
            }

            var getOpenInterest = () =>
            {
                var startDate = FactSetUtils.ParseDate(startTime);
                var endDate = FactSetUtils.ParseDate(endTime);

                var volumeRequest = new OptionsVolumeRequest(new() { factSetSymbol }, startDate, endDate, Frequency.D);

                CheckRequestRate();
                return _pricesVolumeApi.GetOptionsVolumeForList(volumeRequest).Data;
            };

            if (!TryRequest(getOpenInterest, out var result, out var exception))
            {
                Log.Error(exception, $"FactSetApi.GetDailyOpenInterest(): Error requesting option volume for {symbol}: {exception.Message}");
                return null;
            }

            // When data is requested for an invalid symbol, FactSet returns a single record with all fields set to null, except for the request id
            if (result.Count == 1 && result[0].FsymId == null)
            {
                Log.Error($"FactSetApi.GetDailyOpenInterest(): No data found for {symbol} between {startTime} and {endTime}");
                return null;
            }

            StoreRawDailyOptionVolumes(symbol, result);

            return result
                .Select(point => new OpenInterest(point.Date.GetValueOrDefault(), symbol, point.OpenInterest.GetValueOrDefault()))
                .Where(tick => tick.EndTime >= startTime && tick.Time <= endTime);
        }

        /// <summary>
        /// Try the given callback while getting a "request timeout" or "too many requests" response multiple times,
        /// returning the result or exception if the max attempts are reached
        /// </summary>
        private bool TryRequest<T>(Func<T> request, out T result, out Exception? exception)
        {
            result = default;
            exception = null;

            const int maxAttempts = 5;
            for (var i = 1; i <= maxAttempts; i++)
            {
                try
                {
                    result = request();
                    return true;
                }
                catch (Exception e)
                {
                    var apiError = default(ApiException);
                    if (e is ApiException apiException)
                    {
                        apiError = apiException;
                    }
                    else if (e is AggregateException aggregateEx)
                    {
                        apiError = aggregateEx.InnerExceptions.OfType<ApiException>().FirstOrDefault();
                    }

                    if (apiError != null && i < maxAttempts)
                    {
                        if (apiError.ErrorCode == (int)HttpStatusCode.TooManyRequests)
                        {
                            Log.Error(apiError, $"FactSet API Rate limit exceeded. Waiting 5 seconds before retrying (attempt {i + 1}/{maxAttempts}).");
                            Thread.Sleep(5000);
                            continue;
                        }

                        // During Beta period, options chain and options screening request might time out at 30s.
                        // Reference: https://developer.factset.com/api-catalog/factset-options-api
                        if (apiError.ErrorCode == (int)HttpStatusCode.RequestTimeout)
                        {
                            Log.Error(apiError, $"FactSet API request timed out. Waiting 5 seconds before retrying (attempt {i + 1}/{maxAttempts}).");
                            Thread.Sleep(5000);
                            continue;
                        }
                    }

                    exception = apiError ?? e;
                    return false;
                }
            }

            return false;
        }

        private void CheckRequestRate()
        {
            if (_rateLimiter.IsRateLimited)
            {
                Log.Trace("FactSetApi.CheckRequestRate(): Rest API calls are limited; waiting to proceed.");
            }
            _rateLimiter.WaitToProceed();
        }

        /// <summary>
        /// Tries to get the FactSet FOS symbol for the specified symbol
        /// </summary>
        private bool TryGetFactSetFosSymbol(Symbol symbol, out string factSetFosSymbol)
        {
            factSetFosSymbol = _symbolMapper.GetFactSetFosSymbol(symbol);
            if (factSetFosSymbol == null)
            {
                Log.Error($"FactSetApi.TryGetFactSetFosSymbol(): Unable to map symbol {symbol} to a FactSet FOS symbol.");
                return false;
            }
            return true;
        }

        #region Raw Data Storage

        private void StoreRawDailyOptionPrices(Symbol symbol, List<OptionsPrices> prices)
        {
            if (!ShouldStoreRawData)
            {
                return;
            }

            var folder = GetRawDataFolder(symbol, Resolution.Daily);
            if (!Directory.Exists(folder))
            {
                Directory.CreateDirectory(folder);
            }
            var pricesZipFile = Path.Combine(folder, "prices.zip");
            var pricesZipFileEntryName = symbol.Value.Replace(" ", "") + ".json";

            _keySynchronizer.Execute(pricesZipFileEntryName, singleExecution: false, () =>
            {
                if (ZipHasEntry(pricesZipFile, pricesZipFileEntryName))
                {
                    return;
                }

                using var streamWriter = new ZipStreamWriter(pricesZipFile, pricesZipFileEntryName);
                streamWriter.WriteLine(JsonConvert.SerializeObject(prices, Formatting.None));
            });
        }

        private void StoreRawDailyOptionVolumes(Symbol symbol, List<OptionsVolume> volumes)
        {
            if (!ShouldStoreRawData)
            {
                return;
            }

            var folder = GetRawDataFolder(symbol, Resolution.Daily);
            var volumesZipFile = Path.Combine(folder, "volumes.zip");
            var volumesZipFileEntryName = symbol.Value.Replace(" ", "") + ".json";

            _keySynchronizer.Execute(volumesZipFile, singleExecution: false, () =>
            {
                if (ZipHasEntry(volumesZipFile, volumesZipFileEntryName))
                {
                    return;
                }

                using var streamWriter = new ZipStreamWriter(volumesZipFile, volumesZipFileEntryName);
                streamWriter.WriteLine(JsonConvert.SerializeObject(volumes, Formatting.None));
            });
        }

        private string GetRawDataFolder(Symbol symbol, Resolution resolution)
        {
            return Path.Combine(_rawDataFolder,
                symbol.SecurityType.ToLower(),
                symbol.ID.Market.ToLower(),
                resolution.ToLower(),
                symbol.ID.Symbol.ToLower());
        }

        private static bool ZipHasEntry(string zipPath, string entryFilename)
        {
            if (!File.Exists(zipPath))
            {
                return false;
        }

            using var archive = ZipFile.OpenRead(zipPath);
            foreach (var entry in archive.Entries)
        {
                if (entry.FullName == entryFilename)
            {
                    return true;
                }
            }

                return false;
            }

        #endregion
    }
}
