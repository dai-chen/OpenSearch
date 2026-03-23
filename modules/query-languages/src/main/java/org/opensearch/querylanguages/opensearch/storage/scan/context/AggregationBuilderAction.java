/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.scan.context;

/** A lambda action to apply on the {@link AggPushDownAction} */
public interface AggregationBuilderAction extends AbstractAction<AggPushDownAction> {
  /**
   * Apply the action on the target {@link AggPushDownAction} and add the operation to the context
   *
   * @param context the context to add the operation to
   * @param operation the operation to add to the context
   */
  default void transform(PushDownContext context, PushDownOperation operation) {
    apply(context.getAggPushDownAction());
    context.getOperationsForAgg().add(operation);
  }
}
