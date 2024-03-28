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

namespace QuantConnect.Lean.DataSource.FactSet
{
    /// <summary>
    /// Wrapper around FactSet APIs
    /// </summary>
    public class FactSetApi : IDisposable
    {
        private const int MaxOptionChainRequestAttepmts = 6;

        private FactSetAuthenticationConfiguration _factSetAuthConfiguration;
        private FactSetOptionsClientConfiguration _factSetOptionsClientConfig;
        private PricesVolumeApi _pricesVolumeApi;
        private OptionChainsScreeningApi _optionChainsScreeningApi;
        private ReferenceApi _optionsReferenceApi;

        private FactSetSymbolMapper _symbolMapper;

        private RateGate _rateLimiter = new(1, TimeSpan.FromSeconds(1));

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
                .Select(optionRight =>
                {
                    // 0=Call, 1=Put
                    var factSetOptionRight = optionRight == OptionRight.Call ? "0" : "1";
                    var request = new OptionScreeningRequest(ExchangeScreeningId.ALLUSAOPTS,
                        OptionScreeningRequest.ConditionOneEnum.UNDERLYINGSECURITYE, underlying.Value,
                        OptionScreeningRequest.ConditionTwoEnum.OPTIONTYPEE, optionType,
                        OptionScreeningRequest.ConditionThreeEnum.CALLORPUTE, factSetOptionRight,
                        date: FactSetUtils.ParseDate(date));

                    CheckRequestRate();
                    return _optionChainsScreeningApi.GetOptionsScreeningForListAsync(request);
                })
                .ToArray();

            try
            {
                Task.WaitAll(tasks);
            }
            catch (AggregateException e)
            {
                var exception = e.InnerExceptions.FirstOrDefault(x => x is ApiException);
                if (exception != null)
                {
                    Log.Error(exception, $"FactSetApi.GetOptionsChain(): Error requesting option chain for {underlying}: {exception.Message}");
                    return Enumerable.Empty<Symbol>();
                }
            }

            var optionsFosIds = tasks.SelectMany(task => task.Result.Data).Select(optionScreening => optionScreening.OptionId);
            if (canonical != null)
            {
                optionsFosIds = optionsFosIds.Where(fosId => fosId.Split("#")[0] == canonical.ID.Symbol);
            }

            var optionSecurityType = Symbol.GetOptionTypeFromUnderlying(underlying.SecurityType);

            return _symbolMapper.GetLeanSymbolsFromFactSetFosSymbols(optionsFosIds.ToList(), optionSecurityType) ?? Enumerable.Empty<Symbol>();
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
            var finishedEvents = Enumerable.Range(1, batchCount).Select(_ => new ManualResetEventSlim(false)).ToList();

            Task.Run(() => Parallel.ForEach(Enumerable.Range(0, batchCount), new ParallelOptions() { MaxDegreeOfParallelism = 10 }, (batchIndex) =>
            {
                var batch = factSetSymbols.Skip(batchIndex * BatchSize).Take(BatchSize).ToList();
                var details = GetOptionDetailsImpl(batch);

                if (details == null)
                {
                    // TODO: Should we stop all requests and throw/return null?
                    Log.Error($"FactSetSymbolMapper.GetLeanSymbolsFromFactSetFosSymbols(): " +
                        $"[Batch #{batchIndex + 1}] Could not fetch Lean symbols for the given FOS symbols. The API returned null.");

                    finishedEvents[batchIndex].Set();

                    return;
                }

                detailsBatches[batchIndex] = details;
                finishedEvents[batchIndex].Set();
            }));

            for (var i = 0; i < batchCount; i++)
            {
                finishedEvents[i].Wait();

                foreach (var symbol in detailsBatches[i])
                {
                    yield return symbol;
                }

                // We can free the memory used by this batch
                detailsBatches[i] = null;
            }

            foreach (var e in finishedEvents)
            {
                e.DisposeSafely();
            }
        }

        private IEnumerable<OptionsReferences>? GetOptionDetailsImpl(List<string> factSetSymbols)
        {
            var request = new OptionsReferencesRequest(factSetSymbols);

            try
            {
                CheckRequestRate();
                var response = _optionsReferenceApi.GetOptionsReferencesForList(request);
                return response.Data;
            }
            catch (ApiException e)
            {
                Log.Error(e, $"FactSetApi.GetOptionDetails(): Error requesting option details for given FOS symbols: {e.Message}");
                return null;
            }
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

            CheckRequestRate();
            var symbolList = new List<string>() { factSetSymbol };
            var pricesRequest = new OptionsPricesRequest(symbolList, startDate, endDate, Frequency.D);
            var pricesResponseTask = _pricesVolumeApi.GetOptionsPricesForListAsync(pricesRequest);

            CheckRequestRate();
            var volumeRequest = new OptionsVolumeRequest(symbolList, startDate, endDate, Frequency.D);
            var volumeResponseTask = _pricesVolumeApi.GetOptionsVolumeForListAsync(volumeRequest);

            try
            {
                Task.WaitAll(pricesResponseTask, volumeResponseTask);
            }
            catch (AggregateException e)
            {
                var exception = e.InnerExceptions.FirstOrDefault(x => x is ApiException);
                if (exception != null)
                {
                    Log.Error(exception, $"FactSetApi.GetDailyOptionsTrades(): Error requesting option prices for {symbol}: {exception.Message}");
                    return null;
                }
            }

            var pricesResponse = pricesResponseTask.Result;
            var volumeResponse = volumeResponseTask.Result;

            var prices = pricesResponse.Data;
            var volumes = volumeResponse.Data;

            // When data is requested for an invalid symbol, FactSet returns a single record with all fields set to null, except for the request id
            if (prices.Count == 1 && prices[0].FsymId == null)
            {
                Log.Error($"FactSetApi.GetDailyOptionsTrades(): No data found for {symbol} between {startTime} and {endTime}");
                return null;
            }

            StoreRawDailyOptionPrices(symbol, prices);

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

                yield return new TradeBar(priceDate,
                    symbol,
                    (decimal)priceData.PriceOpen.GetValueOrDefault(),
                    (decimal)priceData.PriceHigh.GetValueOrDefault(),
                    (decimal)priceData.PriceLow.GetValueOrDefault(),
                    (decimal)priceData.Price.GetValueOrDefault(),
                    (decimal)volumeData.Volume.GetValueOrDefault(),
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

            var startDate = FactSetUtils.ParseDate(startTime);
            var endDate = FactSetUtils.ParseDate(endTime);

            var volumeRequest = new OptionsVolumeRequest(new() { factSetSymbol }, startDate, endDate, Frequency.D);
            OptionsVolumeResponse volumeResponse;

            try
            {
                CheckRequestRate();
                volumeResponse = _pricesVolumeApi.GetOptionsVolumeForList(volumeRequest);
            }
            catch (ApiException e)
            {
                Log.Error(e, $"FactSetApi.GetDailyOpenInterest(): Error requesting option volume for {symbol}: {e.Message}");
                return null;
            }

            var volumes = volumeResponse.Data;

            // When data is requested for an invalid symbol, FactSet returns a single record with all fields set to null, except for the request id
            if (volumes.Count == 1 && volumes[0].FsymId == null)
            {
                Log.Error($"FactSetApi.GetDailyOpenInterest(): No data found for {symbol} between {startTime} and {endTime}");
                return null;
            }

            StoreRawDailyOptionVolumes(symbol, volumes);

            return volumes
                .Select(point => new OpenInterest(point.Date.GetValueOrDefault(), symbol, point.OpenInterest.GetValueOrDefault()))
                .Where(tick => tick.EndTime >= startTime && tick.Time <= endTime);
        }

        private void StoreRawDailyOptionPrices(Symbol symbol, List<OptionsPrices> prices)
        {
            if (!ShouldStoreRawData)
            {
                return;
            }

            var folder = GetFolder(symbol, Resolution.Daily);
            if (!Directory.Exists(folder))
            {
                Directory.CreateDirectory(folder);
            }
            var pricesZipFile = Path.Combine(folder, "prices.zip");
            var pricesJsonFileName = "prices.json";
            using var streamWriter = new ZipStreamWriter(pricesZipFile, pricesJsonFileName);
            streamWriter.WriteLine(JsonConvert.SerializeObject(prices, Formatting.None));
        }

        private void StoreRawDailyOptionVolumes(Symbol symbol, List<OptionsVolume> volumes)
        {
            if (!ShouldStoreRawData)
            {
                return;
            }

            var folder = GetFolder(symbol, Resolution.Daily);
            var volumesZipFile = Path.Combine(folder, "volumes.zip");
            var volumesJsonFileName = "volumes.json";
            using var streamWriter = new ZipStreamWriter(volumesZipFile, volumesJsonFileName);
            streamWriter.WriteLine(JsonConvert.SerializeObject(volumes, Formatting.None));
        }

        private string GetFolder(Symbol symbol, Resolution resolution)
        {
            return Path.Combine(_rawDataFolder,
                symbol.SecurityType.ToLower(),
                symbol.ID.Market.ToLower(),
                Resolution.Daily.ToLower(),
                symbol.Underlying.Value.ToLowerInvariant());
        }

        private void CheckRequestRate()
        {
            if (_rateLimiter.IsRateLimited)
            {
                Log.Trace("FactSetApi.CheckRequestRate(): Rest API calls are limited; waiting to proceed.");
            }
            _rateLimiter.WaitToProceed();
        }

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
    }
}
