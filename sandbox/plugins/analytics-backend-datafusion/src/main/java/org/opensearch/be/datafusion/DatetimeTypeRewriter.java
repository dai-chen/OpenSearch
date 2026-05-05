/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Rewrites a RelNode plan to:
 * 1. Replace UDT return types on PPL datetime function RexCalls with standard Calcite datetime types
 * 2. Add a final LogicalProject that casts all datetime output fields to VARCHAR
 *
 * This enables clean Substrait conversion (isthmus handles standard types natively)
 * and satisfies PPL V2's requirement that datetime results are returned as strings.
 */
public class DatetimeTypeRewriter {

    // PPL datetime functions whose return type should be replaced
    private static final Set<String> DATETIME_FUNCTIONS = Set.of(
        "DATE_ADD", "LAST_DAY", "DATE", "TIMESTAMP", "NOW"
    );

    // Map function name to its standard Calcite return type
    private static SqlTypeName standardReturnType(String functionName) {
        return switch (functionName.toUpperCase()) {
            case "DATE" -> SqlTypeName.DATE;
            case "DATE_ADD", "LAST_DAY", "TIMESTAMP", "NOW" -> SqlTypeName.TIMESTAMP;
            default -> null;
        };
    }

    /**
     * Rewrite the plan: replace UDT types and add final VARCHAR cast project.
     */
    public static RelNode rewrite(RelNode plan) {
        RexBuilder rexBuilder = plan.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = plan.getCluster().getTypeFactory();

        // Step 1: Replace UDT return types with standard Calcite types
        RelNode rewritten = plan.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                return visited.accept(new DatetimeRexShuttle(rexBuilder, typeFactory));
            }
        });

        // Step 2: Add final Project with CAST(datetime -> VARCHAR) for output
        return addOutputCastProject(rewritten, rexBuilder, typeFactory);
    }

    private static RelNode addOutputCastProject(RelNode plan, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
        List<RexNode> projects = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        boolean hasCast = false;

        for (RelDataTypeField field : plan.getRowType().getFieldList()) {
            RexNode ref = rexBuilder.makeInputRef(field.getType(), field.getIndex());
            if (SqlTypeFamily.DATETIME.getTypeNames().contains(field.getType().getSqlTypeName())) {
                RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
                ref = rexBuilder.makeCast(varcharType, ref);
                hasCast = true;
            }
            projects.add(ref);
            fieldNames.add(field.getName());
        }

        if (hasCast == false) {
            return plan; // No datetime fields in output, no need for extra project
        }

        return LogicalProject.create(plan, List.of(), projects, fieldNames);
    }

    /**
     * RexShuttle that replaces UDT return types on datetime function calls.
     */
    private static class DatetimeRexShuttle extends RexShuttle {
        private final RexBuilder rexBuilder;
        private final RelDataTypeFactory typeFactory;

        DatetimeRexShuttle(RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            this.rexBuilder = rexBuilder;
            this.typeFactory = typeFactory;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            // Visit children first
            RexCall visited = (RexCall) super.visitCall(call);

            String opName = visited.getOperator().getName().toUpperCase();
            if (DATETIME_FUNCTIONS.contains(opName)) {
                SqlTypeName targetType = standardReturnType(opName);
                if (targetType != null) {
                    RelDataType newType = typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(targetType),
                        visited.getType().isNullable()
                    );
                    // Only replace if the current type is not already the standard type
                    if (visited.getType().getSqlTypeName() != targetType) {
                        return rexBuilder.makeCall(newType, visited.getOperator(), visited.getOperands());
                    }
                }
            }
            return visited;
        }
    }
}
