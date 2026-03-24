/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.scan.context;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.querylanguages.opensearch.storage.scan.AbstractCalciteIndexScan;

import java.util.List;
import java.util.Locale;

/**
 * Information about a sort expression pushed down to OpenSearch.
 *
 * @opensearch.internal
 */
public class SortExprDigest {
    private final RexNode expression;
    private final String fieldName;
    private final RelFieldCollation.Direction direction;
    private final RelFieldCollation.NullDirection nullDirection;

    /**
     * Full constructor.
     *
     * @param expression the RexNode expression (nullable for simple field refs)
     * @param fieldName the field name (nullable for complex expressions)
     * @param direction sort direction
     * @param nullDirection null handling direction
     */
    public SortExprDigest(RexNode expression, String fieldName, Direction direction, NullDirection nullDirection) {
        this.expression = expression;
        this.fieldName = fieldName;
        this.direction = direction;
        this.nullDirection = nullDirection;
    }

    /**
     * Constructor for complex expressions.
     *
     * @param expression the RexNode expression
     * @param direction sort direction
     * @param nullDirection null handling direction
     */
    public SortExprDigest(RexNode expression, Direction direction, NullDirection nullDirection) {
        this(expression, null, direction, nullDirection);
    }

    /**
     * Constructor for simple field references.
     *
     * @param fieldName the field name
     * @param direction sort direction
     * @param nullDirection null handling direction
     */
    public SortExprDigest(String fieldName, Direction direction, NullDirection nullDirection) {
        this(null, fieldName, direction, nullDirection);
    }

    /** @return the expression */
    public RexNode getExpression() {
        return expression;
    }

    /** @return the field name */
    public String getFieldName() {
        return fieldName;
    }

    /** @return the sort direction */
    public Direction getDirection() {
        return direction;
    }

    /** @return the null direction */
    public NullDirection getNullDirection() {
        return nullDirection;
    }

    /**
     * Check if this is a simple field reference.
     *
     * @return true if simple field reference
     */
    public boolean isSimpleFieldReference() {
        return expression == null && !StringUtils.isEmpty(fieldName);
    }

    /**
     * Get the effective expression for this sort info.
     *
     * @param scan the scan to get schema from
     * @return the RexNode expression
     */
    public RexNode getEffectiveExpression(AbstractCalciteIndexScan scan) {
        if (isSimpleFieldReference()) {
            List<String> currentFieldNames = scan.getRowType().getFieldNames();
            int fieldIndex = currentFieldNames.indexOf(fieldName);
            if (fieldIndex >= 0) {
                return scan.getCluster()
                    .getRexBuilder()
                    .makeInputRef(scan.getRowType().getFieldList().get(fieldIndex).getType(), fieldIndex);
            }
            return null;
        }
        return expression;
    }

    @Override
    public String toString() {
        String sortTarget = isSimpleFieldReference() ? fieldName : expression.toString();
        return String.format(Locale.ROOT, "%s %s NULLS_%s", sortTarget, direction.toString(), nullDirection.toString());
    }

    /**
     * Check if missing values should sort to max.
     *
     * @return true if missing is max
     */
    public boolean isMissingMax() {
        return (direction == Direction.ASCENDING) ^ (nullDirection == NullDirection.FIRST);
    }
}
