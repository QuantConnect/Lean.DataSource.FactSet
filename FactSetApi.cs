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
using System.Threading.Tasks;
using System.IO;
using System.Net;
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
using FactSetOptionsExchange = FactSet.SDK.FactSetOptions.Model.Exchange;

namespace QuantConnect.Lean.DataSource.FactSet
{
    /// <summary>
    /// Wrapper around FactSet APIs
    /// </summary>
    public class FactSetApi
    {
        private const int MaxOptionChainRequestAttepmts = 6;

        private FactSetAuthenticationConfiguration _factSetAuthConfiguration;
        private FactSetOptionsClientConfiguration _factSetOptionsClientConfig;
        private PricesVolumeApi _pricesVolumeApi;
        private OptionChainsScreeningApi _optionChainsScreeningApi;
        private ReferenceApi _optionsReferenceApi;

        private FactSetSymbolMapper _symbolMapper;

        private string? _rawDataFolder;

        private bool ShouldStoreRawData => !string.IsNullOrEmpty(_rawDataFolder);

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
        /// Gets the option chain for the specified symbol and date
        /// </summary>
        /// <param name="underlying">The underlying symbol</param>
        /// <param name="date">The date</param>
        /// <returns>The option contract list</returns>
        public IEnumerable<Symbol> GetOptionsChain(Symbol underlying, DateTime date)
        {
            var factSetSymbol = _symbolMapper.GetFactSetOCC21Symbol(underlying);
            var request = new ChainsRequest(new List<string>() { factSetSymbol }, FactSetUtils.ParseDate(date), IdType.OCC21,
                FactSetOptionsExchange.USA);

            var response = default(ChainsResponse);
            var attempts = 0;
            while (attempts < MaxOptionChainRequestAttepmts)
            {
                try
                {
                    attempts++;
                    response = _optionChainsScreeningApi.GetOptionsChainsForList(request);
                }
                catch (ApiException e)
                {
                    // During Beta period, options chain and options screening request might time out at 30s.
                    // Reference: https://developer.factset.com/api-catalog/factset-options-api
                    if (e.ErrorCode == (int)HttpStatusCode.RequestTimeout)
                    {
                        Log.Trace($"FactSetApi.GetOptionsChains(): Attempt {attempts}/{MaxOptionChainRequestAttepmts} " +
                            $"to get option chain for underlying timed out. Trying again...");
                        continue;
                    }

                    Log.Error(e, $"FactSetApi.GetOptionsChains(): Exception when requesting option chains for underlying {underlying}: {e.Message}");
                    yield break;
                }
            }

            if (response == null)
            {
                yield break;
            }

            var optionsSecurityType = underlying.SecurityType == SecurityType.Index ? SecurityType.IndexOption : SecurityType.Option;
            foreach (var data in response.Data)
            {
                var optionSymbol = _symbolMapper.GetLeanSymbol(data.OptionId, optionsSecurityType, Market.USA);
                yield return optionSymbol;
            }
        }

        /// <summary>
        /// Returns basic reference details for the option
        /// </summary>
        /// <param name="option">The option which details are requested</param>
        /// <returns>The option reference details. Null if the request fails.</returns>
        public OptionsReferences? GetOptionDetails(Symbol option)
        {
            var factSetSymbol = _symbolMapper.GetFactSetOCC21Symbol(option);
            var request = new OptionsReferencesRequest(new List<string>() { factSetSymbol });

            try
            {
                var response = _optionsReferenceApi.GetOptionsReferencesForList(request);
                return response.Data.SingleOrDefault();
            }
            catch (ApiException e)
            {
                Log.Error(e, $"FactSetApi.GetOptionReferences(): Error requesting option details for {option}: {e.Message}");
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

            var pricesRequest = new OptionsPricesRequest(new() { factSetSymbol }, startDate, endDate, Frequency.D);
            var pricesResponseTask = _pricesVolumeApi.GetOptionsPricesForListAsync(pricesRequest);

            var volumeRequest = new OptionsVolumeRequest(new() { factSetSymbol }, startDate, endDate, Frequency.D);
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
