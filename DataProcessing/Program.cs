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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using FactSet.SDK.Utils.Authentication;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Securities.IndexOption;
using FactSetAuthenticationConfiguration = FactSet.SDK.Utils.Authentication.Configuration;

namespace QuantConnect.DataProcessing
{
    public class Program
    {
        public static string DataFleetDeploymentDate = "QC_DATAFLEET_DEPLOYMENT_DATE";

        public static void Main()
        {
            var ticker = Config.Get("tickers");
            var securityType = Config.GetValue("security-type", SecurityType.IndexOption);

            SecurityType underlyingSecurityType;
            try
            {
                underlyingSecurityType = Symbol.GetUnderlyingFromOptionType(securityType);
            }
            catch (Exception ex)
            {
                Log.Error($"QuantConnect.DataProcessing.Program.Main(): {ex.Message}");
                    Environment.Exit(1);
                    return;
            }

            var underlyingTicker = IndexOptionSymbol.MapToUnderlying(ticker);
            var underlying = Symbol.Create(underlyingTicker, underlyingSecurityType, Market.USA);
            var symbol = Symbol.CreateCanonicalOption(underlying, ticker, Market.USA, null);

            var resolution = Config.GetValue("resolution", Resolution.Daily);

            var startDate = Config.GetValue<DateTime>("start-date");
            var endDate = startDate;
            if (startDate != default)
            {
                endDate = DateTime.UtcNow.Date.AddDays(-1);
            }
            else
            {
                var startDateStr = Environment.GetEnvironmentVariable(DataFleetDeploymentDate);
                if (string.IsNullOrEmpty(startDateStr))
                {
                    Log.Error($"QuantConnect.DataProcessing.Program.Main(): The start date was neither set in the configuration " +
                        $"nor in the {DataFleetDeploymentDate} environment variable.");
                    Environment.Exit(1);
                }
                startDate = DateTime.ParseExact(startDateStr, "yyyyMMdd", CultureInfo.InvariantCulture);
            }

            var factSetAuthConfig = JsonConvert.DeserializeObject<FactSetAuthenticationConfiguration>(Config.Get("factset-auth-config"));
            if (factSetAuthConfig == null)
            {
                Log.Error($"QuantConnect.DataProcessing.Program.Main(): The FactSet authentication configuration was not set.");
                Environment.Exit(1);
            }

            var tickerWhitelist = Config.GetValue<List<string>>("factset-ticker-whitelist");

            // Get the config values first before running. These values are set for us
            // automatically to the value set on the website when defining this data type
            var dataFolder = Config.Get("temp-output-directory", "./temp-output-directory");
            var rawDataFolder = Config.Get("raw-data-folder", "./raw");

            Log.Trace($"DataProcessing.Main(): Processing {ticker} | {resolution} | {startDate:yyyy-MM-dd} - {endDate:yyyy-MM-dd}");

            FactSetDataProcessor processor = null;
            try
            {
                processor = new FactSetDataProcessor(factSetAuthConfig, symbol, resolution, startDate, endDate, dataFolder, rawDataFolder,
                    tickerWhitelist);
            }
            catch (Exception err)
            {
                Log.Error(err, $"QuantConnect.DataProcessing.Program.Main(): The downloader/converter failed to be instantiated");
                Environment.Exit(1);
            }

            try
            {
                if (!processor.Run())
                {
                    Log.Error($"QuantConnect.DataProcessing.Program.Main(): Failed to download/process data");
                    Environment.Exit(1);
                }
            }
            catch (Exception err)
            {
                Log.Error(err, $"QuantConnect.DataProcessing.Program.Main(): The downloader/converter exited unexpectedly");
                Environment.Exit(1);
            }
            //finally
            //{
            //    processor.DisposeSafely();
            //}

            Environment.Exit(0);
        }
    }
}