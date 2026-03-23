/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.response.agg;

public class Utils {
    /**
     * Utils to handle Nan/Infinite Value.
     *
     * @return null if is Nan or is +-Infinity.
     */
    public static Object handleNanInfValue(double value) {
        return Double.isNaN(value) || Double.isInfinite(value) ? null : value;
    }
}
