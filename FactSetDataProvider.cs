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
using NodaTime;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QuantConnect.Data;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.HistoricalData;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Configuration;
using QuantConnect.Packets;
using FactSetAuthenticationConfiguration = FactSet.SDK.Utils.Authentication.Configuration;
using QuantConnect.Api;
using RestSharp;
using System.IO;
using System.Net.NetworkInformation;
using System.Security.Cryptography;
using System.Text;
using QuantConnect.Util;
using System.Net;
using System.Linq;

namespace QuantConnect.Lean.DataSource.FactSet
{
    /// <summary>
    /// History provider implementation for FactSet
    /// </summary>
    public class FactSetDataProvider : SynchronizingHistoryProvider, IDisposable
    {
        private static SecurityType[] _supportedSecurityTypes = new[] { SecurityType.IndexOption };

        private FactSetAuthenticationConfiguration? _factSetAuthConfiguration;
        private FactSetApi? _factSetApi;
        private FactSetSymbolMapper? _symbolMapper;
        private IOptionChainProvider? _optionChainProvider;

        private bool _initialized;

        private string? _rawDataFolder;

        private bool _unsupportedAssetWarningSent;
        private bool _unsupportedSecurityTypeWarningSent;
        private bool _unsupportedResolutionWarningSent;
        private bool _invalidDateRangeWarningSent;
        private bool _futureDataWarningSent;

        /// <summary>
        /// Creates a new instance of the <see cref="FactSetDataProvider"/> class
        /// </summary>
        /// <param name="factSetAuthConfiguration">The FactSet authentication configuration to use for their API</param>
        /// <param name="rawDataFolder">The raw data folder</param>
        protected internal FactSetDataProvider(FactSetAuthenticationConfiguration? factSetAuthConfiguration, string? rawDataFolder = null)
        {
            _factSetAuthConfiguration = factSetAuthConfiguration;
            _rawDataFolder = rawDataFolder;
        }

        /// <summary>
        /// Creates a new instance of the <see cref="FactSetDataProvider"/> class
        /// </summary>
        /// <param name="factSetAuthConfig">The FactSet authentication configuration as a JSON object</param>
        public FactSetDataProvider(JObject factSetAuthConfig)
            : this(factSetAuthConfig.ToObject<FactSetAuthenticationConfiguration>())
        {
        }

        /// <summary>
        /// Creates a new instance of the <see cref="FactSetDataProvider"/> class.
        /// The FactSet authentication configuration is read from the configuration file.
        /// </summary>
        public FactSetDataProvider()
            : this(GetFactSetAuthConfigFromLeanConfig())
        {
        }

        /// <summary>
        /// Disposes of the resources
        /// </summary>
        public void Dispose()
        {
            _factSetApi.DisposeSafely();
        }

        /// <summary>
        /// Initializes the history provider for the specified job
        /// </summary>
        /// <param name="parameters">The intialization parameters</param>
        public override void Initialize(HistoryProviderInitializeParameters parameters)
        {
            if (_initialized)
            {
                return;
            }

            if (_factSetAuthConfiguration == null && parameters.Job is LiveNodePacket job)
            {
                if (!job.BrokerageData.TryGetValue("factset-auth-config", out var factSetAuthConfigStr))
                {
                    if (job.BrokerageData.TryGetValue("factset-auth-config-file", out var factSetAuthConfigFilePath))
                    {
                        factSetAuthConfigStr = File.ReadAllText(factSetAuthConfigFilePath);
                    }
                }

                if (!string.IsNullOrEmpty(factSetAuthConfigStr))
                {
                    _factSetAuthConfiguration = JsonConvert.DeserializeObject<FactSetAuthenticationConfiguration>(factSetAuthConfigStr);
                }
            }

            Initialize();
        }

        /// <summary>
        /// Initializes the history provider
        /// </summary>
        private void Initialize()
        {
            if (_initialized)
            {
                return;
            }

            if (_factSetAuthConfiguration == null)
            {
                throw new InvalidOperationException("The FactSet authentication configuration is required.");
            }

            ValidateSubscription();

            _symbolMapper = new FactSetSymbolMapper();
            _factSetApi = new FactSetApi(_factSetAuthConfiguration, _symbolMapper, _rawDataFolder);
            _symbolMapper.SetApi(_factSetApi);
            _optionChainProvider = new CachingOptionChainProvider(new FactSetOptionChainProvider(_factSetApi));
            _initialized = true;
        }

        /// <summary>
        /// Gets the history for the requested securities
        /// </summary>
        /// <param name="requests">The historical data requests</param>
        /// <param name="sliceTimeZone">The time zone used when time stamping the slice instances</param>
        /// <returns>An enumerable of the slices of data covering the span specified in each request</returns>
        public override IEnumerable<Slice>? GetHistory(IEnumerable<Data.HistoryRequest> requests, DateTimeZone sliceTimeZone)
        {
            if (!_initialized)
            {
                throw new InvalidOperationException("The history provider has not been initialized.");
            }

            // Create subscription objects from the configs
            var subscriptions = new List<Subscription>();
            foreach (var request in requests)
            {
                // Retrieve the history for the current request
                var history = GetHistory(request);

                if (history == null)
                {
                    // If history is null, it indicates that the request contains wrong parameters
                    // Handle the case where the request parameters are incorrect
                    continue;
                }

                var subscription = CreateSubscription(request, history);
                subscriptions.Add(subscription);
            }

            // Validate that at least one subscription is valid; otherwise, return null
            if (subscriptions.Count == 0)
            {
                return null;
            }

            return CreateSliceEnumerableFromSubscriptions(subscriptions, sliceTimeZone);
        }

        /// <summary>
        /// Gets the history for the requested security
        /// </summary>
        /// <param name="request">The historical data request</param>
        /// <returns>An enumerable of BaseData points</returns>
        public IEnumerable<BaseData>? GetHistory(Data.HistoryRequest request)
        {
            if (!_initialized)
            {
                throw new InvalidOperationException("The history provider has not been initialized.");
            }

            if (!IsValidRequest(request))
            {
                return null;
            }

            Log.Trace($"FactSetDataProvider.GetHistory(): Fetching historical data for {request.Symbol.Value} {request.Resolution} " +
                $"{request.StartTimeUtc} - {request.EndTimeUtc}");

            return request.TickType switch
            {
                TickType.Trade => _factSetApi?.GetDailyOptionsTrades(request.Symbol, request.StartTimeUtc, request.EndTimeUtc),
                TickType.Quote => _factSetApi?.GetDailyOptionsQuotes(request.Symbol, request.StartTimeUtc, request.EndTimeUtc),
                TickType.OpenInterest => _factSetApi?.GetDailyOpenInterest(request.Symbol, request.StartTimeUtc, request.EndTimeUtc),
                _ => throw new ArgumentOutOfRangeException(nameof(request.TickType), request.TickType, null)
            };
        }

        /// <summary>
        /// Method returns a collection of symbols that are available at the broker.
        /// </summary>
        /// <param name="symbol">Symbol to search option chain for</param>
        /// <param name="date">Reference date</param>
        /// <returns>Option chain associated with the provided symbol</returns>
        public IEnumerable<Symbol> GetOptionChain(Symbol symbol, DateTime date)
        {
            if (!_initialized)
            {
                throw new InvalidOperationException("The history provider has not been initialized.");
            }

            if ((symbol.SecurityType.IsOption() && symbol.SecurityType == SecurityType.FutureOption) ||
                (symbol.HasUnderlying && symbol.Underlying.SecurityType != SecurityType.Equity && symbol.Underlying.SecurityType != SecurityType.Index))
            {
                throw new ArgumentException($"Unsupported security type {symbol.SecurityType}");
            }

            Log.Trace($"FactSetDataProvider.GetOptionChain(): Requesting option chain for {symbol.Value} - {date}");

            return _optionChainProvider.GetOptionContractList(symbol, date);
        }

        /// <summary>
        /// Checks if this data provider supports the specified request
        /// </summary>
        /// <param name="request">The history request</param>
        /// <returns>returns true if Data Provider supports the specified request; otherwise false</returns>
        public bool IsValidRequest(Data.HistoryRequest request)
        {
            return IsValidRequest(request.Symbol, request.Resolution, request.StartTimeUtc, request.EndTimeUtc) && !request.Symbol.IsCanonical();
        }

        /// <summary>
        /// Checks if this data provider supports requesting data for the specified symbol and resolution
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <param name="resolution">The resolution</param>
        /// <param name="startTimeUtc">The UTC start date</param>
        /// <param name="endTimeUtc">The UTC end date</param>
        /// <returns>returns true if Data Provider supports the specified request; otherwise false</returns>
        public bool IsValidRequest(Symbol symbol,  Resolution resolution, DateTime startTimeUtc, DateTime endTimeUtc)
        {
            if (symbol.Value.IndexOfInvariant("universe", true) != -1)
            {
                if (!_unsupportedAssetWarningSent)
                {
                    _unsupportedAssetWarningSent = true;
                    Log.Trace($"FactSetDataProvider.IsValidRequest(): Unsupported asset type {symbol}");
                }
                return false;
            }

            if (!_supportedSecurityTypes.Contains(symbol.SecurityType))
            {
                if (!_unsupportedSecurityTypeWarningSent)
                {
                    _unsupportedSecurityTypeWarningSent = true;
                    Log.Trace($"FactSetDataProvider.IsValidRequest(): Unsupported security type {symbol.SecurityType}. " +
                        $"Only {string.Join(", ", _supportedSecurityTypes)} supported");
                }
                return false;
            }

            if (resolution != Resolution.Daily)
            {
                if (!_unsupportedResolutionWarningSent)
                {
                    _unsupportedResolutionWarningSent = true;
                    Log.Trace($"FactSetDataProvider.IsValidRequest(): Unsupported resolution {resolution}. Only {Resolution.Daily} is support");
                }
                return false;
            }

            if (endTimeUtc < startTimeUtc)
            {
                if (!_invalidDateRangeWarningSent)
                {
                    _invalidDateRangeWarningSent = true;
                    Log.Trace($"FactSetDataProvider.IsValidRequest(): Invalid date range {startTimeUtc} - {endTimeUtc}");
                }
                return false;
            }

            var now = DateTime.UtcNow.Date;
            if (startTimeUtc > now || endTimeUtc > now)
            {
                if (!_futureDataWarningSent)
                {
                    _futureDataWarningSent = true;
                    Log.Trace($"FactSetDataProvider.IsValidRequest(): Requesting future data {startTimeUtc} - {endTimeUtc}");
                }
                return false;
            }

            return true;
        }

        /// <summary>
        /// Gets the FactSet authentication configuration from the Lean configuration file, either directly as a json object or from a file.
        /// </summary>
        private static FactSetAuthenticationConfiguration? GetFactSetAuthConfigFromLeanConfig()
        {
            var authConfigStr = Config.Get("factset-auth-config");
            if (string.IsNullOrEmpty(authConfigStr))
            {
                var authConfigFilePath = Config.Get("factset-auth-config-file");
                if (string.IsNullOrEmpty(authConfigFilePath))
                {
                    return null;
                }

                authConfigStr = File.ReadAllText(authConfigFilePath);
            }

            return JsonConvert.DeserializeObject<FactSetAuthenticationConfiguration>(authConfigStr);
        }

        private class ModulesReadLicenseRead : Api.RestResponse
        {
            [JsonProperty(PropertyName = "license")]
            public string License;

            [JsonProperty(PropertyName = "organizationId")]
            public string OrganizationId;
        }

        /// <summary>
        /// Validate the user of this project has permission to be using it via our web API.
        /// </summary>
        private static void ValidateSubscription()
        {
            try
            {
                const int productId = 343;
                var userId = Globals.UserId;
                var token = Globals.UserToken;
                var organizationId = Globals.OrganizationID;
                // Verify we can authenticate with this user and token
                var api = new ApiConnection(userId, token);
                if (!api.Connected)
                {
                    throw new ArgumentException("Invalid api user id or token, cannot authenticate subscription.");
                }
                // Compile the information we want to send when validating
                var information = new Dictionary<string, object>()
                {
                    {"productId", productId},
                    {"machineName", Environment.MachineName},
                    {"userName", Environment.UserName},
                    {"domainName", Environment.UserDomainName},
                    {"os", Environment.OSVersion}
                };
                // IP and Mac Address Information
                try
                {
                    var interfaceDictionary = new List<Dictionary<string, object>>();
                    foreach (var nic in NetworkInterface.GetAllNetworkInterfaces().Where(nic => nic.OperationalStatus == OperationalStatus.Up))
                    {
                        var interfaceInformation = new Dictionary<string, object>();
                        // Get UnicastAddresses
                        var addresses = nic.GetIPProperties().UnicastAddresses
                            .Select(uniAddress => uniAddress.Address)
                            .Where(address => !IPAddress.IsLoopback(address)).Select(x => x.ToString());
                        // If this interface has non-loopback addresses, we will include it
                        if (!addresses.IsNullOrEmpty())
                        {
                            interfaceInformation.Add("unicastAddresses", addresses);
                            // Get MAC address
                            interfaceInformation.Add("MAC", nic.GetPhysicalAddress().ToString());
                            // Add Interface name
                            interfaceInformation.Add("name", nic.Name);
                            // Add these to our dictionary
                            interfaceDictionary.Add(interfaceInformation);
                        }
                    }
                    information.Add("networkInterfaces", interfaceDictionary);
                }
                catch (Exception)
                {
                    // NOP, not necessary to crash if fails to extract and add this information
                }
                // Include our OrganizationId if specified
                if (!string.IsNullOrEmpty(organizationId))
                {
                    information.Add("organizationId", organizationId);
                }
                var request = new RestRequest("modules/license/read", Method.POST) { RequestFormat = DataFormat.Json };
                request.AddParameter("application/json", JsonConvert.SerializeObject(information), ParameterType.RequestBody);
                api.TryRequest(request, out ModulesReadLicenseRead result);
                if (!result.Success)
                {
                    throw new InvalidOperationException($"Request for subscriptions from web failed, Response Errors : {string.Join(',', result.Errors)}");
                }

                var encryptedData = result.License;
                // Decrypt the data we received
                DateTime? expirationDate = null;
                long? stamp = null;
                bool? isValid = null;
                if (encryptedData != null)
                {
                    // Fetch the org id from the response if it was not set, we need it to generate our validation key
                    if (string.IsNullOrEmpty(organizationId))
                    {
                        organizationId = result.OrganizationId;
                    }
                    // Create our combination key
                    var password = $"{token}-{organizationId}";
                    var key = SHA256.HashData(Encoding.UTF8.GetBytes(password));
                    // Split the data
                    var info = encryptedData.Split("::");
                    var buffer = Convert.FromBase64String(info[0]);
                    var iv = Convert.FromBase64String(info[1]);
                    // Decrypt our information
                    using var aes = new AesManaged();
                    var decryptor = aes.CreateDecryptor(key, iv);
                    using var memoryStream = new MemoryStream(buffer);
                    using var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
                    using var streamReader = new StreamReader(cryptoStream);
                    var decryptedData = streamReader.ReadToEnd();
                    if (!decryptedData.IsNullOrEmpty())
                    {
                        var jsonInfo = JsonConvert.DeserializeObject<JObject>(decryptedData);
                        expirationDate = jsonInfo["expiration"]?.Value<DateTime>();
                        isValid = jsonInfo["isValid"]?.Value<bool>();
                        stamp = jsonInfo["stamped"]?.Value<int>();
                    }
                }
                // Validate our conditions
                if (!expirationDate.HasValue || !isValid.HasValue || !stamp.HasValue)
                {
                    throw new InvalidOperationException("Failed to validate subscription.");
                }

                var nowUtc = DateTime.UtcNow;
                var timeSpan = nowUtc - Time.UnixTimeStampToDateTime(stamp.Value);
                if (timeSpan > TimeSpan.FromHours(12))
                {
                    throw new InvalidOperationException("Invalid API response.");
                }
                if (!isValid.Value)
                {
                    throw new ArgumentException($"Your subscription is not valid, please check your product subscriptions on our website.");
                }
                if (expirationDate < nowUtc)
                {
                    throw new ArgumentException($"Your subscription expired {expirationDate}, please renew in order to use this product.");
                }
            }
            catch (Exception e)
            {
                Log.Error($"FactSetDataProvider.ValidateSubscription(): Failed during validation, shutting down. Error : {e.Message}");
                throw;
            }
        }
    }
}
