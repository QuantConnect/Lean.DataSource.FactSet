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
    /// </summary>
    public class FactSetApi
    {
        private FactSetAuthenticationConfiguration _factSetAuthConfiguration;
        private FactSetOptionsClientConfiguration _factSetOptionsClientConfig;
        private PricesVolumeApi _factSetPricesVolumeApi;
        private OptionChainsScreeningApi _factSetOptionChainsScreeningApi;

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

            _factSetPricesVolumeApi = new PricesVolumeApi(_factSetOptionsClientConfig);
            _factSetOptionChainsScreeningApi = new OptionChainsScreeningApi(_factSetOptionsClientConfig);
        }

        /// <summary>
        /// Gets the option chain for the specified symbol and date
        /// </summary>
        /// <param name="underlying">The underlying symbol</param>
        /// <param name="date">The date</param>
        /// <returns>The option contract list</returns>
        public IEnumerable<Symbol> GetOptionsChain(Symbol underlying, DateTime date)
        {
            var factSetSymbol = _symbolMapper.GetBrokerageSymbol(underlying);
            var request = new ChainsRequest(new List<string>() { factSetSymbol }, FactSetUtils.ParseDate(date), IdType.OCC21,
                FactSetOptionsExchange.USA);

            ChainsResponse response;
            try
            {
                response = _factSetOptionChainsScreeningApi.GetOptionsChainsForList(request);
            }
            catch (ApiException e)
            {
                Log.Error(e, $"FactSetApi.GetOptionsChains(): Exception when requesting option chains for underlying {underlying}: {e.Message}");
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
        /// Gets the trades for the specified symbol and date range
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <param name="startTime">The start time</param>
        /// <param name="endTime">The end time</param>
        /// <returns>The list of trade bars for the specified symbol and date range</returns>
        public IEnumerable<TradeBar> GetDailyOptionsTrades(Symbol symbol, DateTime startTime, DateTime endTime)
        {
            var factSetSymbol = _symbolMapper.GetBrokerageSymbol(symbol);
            var startDate = FactSetUtils.ParseDate(startTime);
            var endDate = FactSetUtils.ParseDate(endTime);

            var pricesRequest = new OptionsPricesRequest(new() { factSetSymbol }, startDate, endDate, Frequency.D);
            var pricesResponseTask = _factSetPricesVolumeApi.GetOptionsPricesForListAsync(pricesRequest);

            var volumeRequest = new OptionsVolumeRequest(new() { factSetSymbol }, startDate, endDate, Frequency.D);
            var volumeResponseTask = _factSetPricesVolumeApi.GetOptionsVolumeForListAsync(volumeRequest);

            Task.WaitAll(pricesResponseTask, volumeResponseTask);

            var pricesResponse = pricesResponseTask.Result;
            var volumeResponse = volumeResponseTask.Result;

            var prices = pricesResponse.Data;
            var volumes = volumeResponse.Data;

            StoreRawDailyOptionPrices(symbol, prices);

            // assume prices and volumes have the same length
            for (int i = 0; i < prices.Count; i++)
            {
                var priceData = prices[i];
                var volumeData = volumes[i];
                var date = priceData.Date.GetValueOrDefault();

                yield return new TradeBar(date,
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
        public IEnumerable<OpenInterest> GetDailyOpenInterest(Symbol symbol, DateTime startTime, DateTime endTime)
        {
            var factSetSymbol = _symbolMapper.GetBrokerageSymbol(symbol);
            var startDate = FactSetUtils.ParseDate(startTime);
            var endDate = FactSetUtils.ParseDate(endTime);

            var volumeRequest = new OptionsVolumeRequest(new() { factSetSymbol }, startDate, endDate, Frequency.D);
            var volumeResponse= _factSetPricesVolumeApi.GetOptionsVolumeForList(volumeRequest);

            StoreRawDailyOptionVolumes(symbol, volumeResponse.Data);

            foreach (var data in volumeResponse.Data)
            {
                yield return new OpenInterest(data.Date.GetValueOrDefault(), symbol, data.OpenInterest.GetValueOrDefault());
            }
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
    }
}
