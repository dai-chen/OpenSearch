/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.scan.context;

import org.opensearch.querylanguages.opensearch.request.OpenSearchRequestBuilder;

/** A lambda action to apply on the {@link OpenSearchRequestBuilder} */
public interface OSRequestBuilderAction extends AbstractAction<OpenSearchRequestBuilder> {
    /**
     * Apply the action on the target {@link OpenSearchRequestBuilder} and add the operation to the
     * context
     *
     * @param context the context to add the operation to
     * @param operation the operation to add to the context
     */
    default void transform(PushDownContext context, PushDownOperation operation) {
        apply(context.getRequestBuilder());
        context.getOperationsForRequestBuilder().add(operation);
    }
}
