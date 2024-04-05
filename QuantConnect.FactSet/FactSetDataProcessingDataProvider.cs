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

using FactSetSDK = FactSet.SDK;

namespace QuantConnect.Lean.DataSource.FactSet
{
    /// <summary>
    /// The only purpose of this class is to provide a way to access the <see cref="FactSetDataProvider"/> class and use its constructor
    /// that takes the raw data folder as a parameter.
    ///
    /// This is done this way in order to keep the public interface of the <see cref="FactSetDataProvider"/> clean and simple,
    /// without exposing its additional capabilities of storing the raw data downloaded from FactSet, which is only useful
    /// for the Data Processing program.
    /// </summary>
    public class FactSetDataProcessingDataProvider : FactSetDataProvider
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FactSetDataProcessingDataProvider"/>
        /// </summary>
        public FactSetDataProcessingDataProvider(FactSetSDK.Utils.Authentication.Configuration factSetAuthConfig, string rawDataFolder)
            : base(factSetAuthConfig, rawDataFolder)
        {
        }
    }
}