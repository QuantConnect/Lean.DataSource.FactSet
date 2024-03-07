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

namespace QuantConnect.Lean.DataSource.FactSet
{
    /// <summary>
    /// </summary>
    public static class FactSetUtils
    {
        private static string _factSetDateFormat = "yyyy-MM-dd";

        /// <summary>
        /// Parse a date to the FactSet expected date format
        /// </summary>
        /// <param name="date">The date to parse</param>
        /// <returns>The date in the FactSet expected date format</returns>
        public static string ParseDate(DateTime date)
        {
            return date.ToStringInvariant(_factSetDateFormat);
        }
    }
}