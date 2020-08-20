/**
 * @license
 * Copyright Paperbits. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file and at https://paperbits.io/license/mit.
 */


/**
 * Settings required to bootstrap Newrelic client.
 */
export interface AppInsightsSettings {
    /**
     * App Insights Instrumentation key, e.g. "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX".
     */
    instrumentationKey: string;
}