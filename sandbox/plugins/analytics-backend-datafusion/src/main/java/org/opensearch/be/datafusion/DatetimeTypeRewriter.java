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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Rewrites a RelNode plan to:
 * 1. Replace UDT return types on PPL datetime function RexCalls with standard Calcite datetime types,
 *    AND replace the operator with a canonical singleton so Substrait Sig matching works.
 * 2. Add a final LogicalProject that casts all datetime output fields to VARCHAR.
 */
public class DatetimeTypeRewriter {

    private static final Logger LOGGER = LogManager.getLogger(DatetimeTypeRewriter.class);

    // Canonical operator singletons — these MUST be the same objects used in
    // DataFusionFragmentConvertor.ADDITIONAL_SCALAR_SIGS so that isthmus's
    // Map<SqlOperator, FunctionFinder> lookup succeeds.
    static final SqlFunction DATE_ADD_OP = new SqlFunction(
        "DATE_ADD", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE,
        null, OperandTypes.ANY_ANY, SqlFunctionCategory.TIMEDATE
    );
    static final SqlFunction LAST_DAY_OP = new SqlFunction(
        "LAST_DAY", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE,
        null, OperandTypes.ANY, SqlFunctionCategory.TIMEDATE
    );
    static final SqlFunction DATE_OP = new SqlFunction(
        "DATE", SqlKind.OTHER_FUNCTION, ReturnTypes.DATE_NULLABLE,
        null, OperandTypes.STRING, SqlFunctionCategory.TIMEDATE
    );
    static final SqlFunction TIMESTAMP_OP = new SqlFunction(
        "TIMESTAMP", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP_NULLABLE,
        null, OperandTypes.STRING, SqlFunctionCategory.TIMEDATE
    );
    static final SqlFunction NOW_OP = new SqlFunction(
        "NOW", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP_NULLABLE,
        null, OperandTypes.NILADIC, SqlFunctionCategory.TIMEDATE
    );

    // Map operator name (uppercase) to canonical singleton
    private static final Map<String, SqlFunction> CANONICAL_OPS = Map.of(
        "DATE_ADD", DATE_ADD_OP,
        "LAST_DAY", LAST_DAY_OP,
        "DATE", DATE_OP,
        "TIMESTAMP", TIMESTAMP_OP,
        "NOW", NOW_OP
    );

    private static SqlTypeName standardReturnType(String functionName) {
        return switch (functionName.toUpperCase()) {
            case "DATE" -> SqlTypeName.DATE;
            case "DATE_ADD", "LAST_DAY", "TIMESTAMP", "NOW" -> SqlTypeName.TIMESTAMP;
            default -> null;
        };
    }

    /**
     * Rewrite the plan: replace UDT types/operators and add final VARCHAR cast project.
     */
    public static RelNode rewrite(RelNode plan) {
        LOGGER.info("[DatetimeTypeRewriter] ═══ BEFORE rewrite ═══\n{}", plan.explain());

        RexBuilder rexBuilder = plan.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = plan.getCluster().getTypeFactory();

        // Step 1: Replace UDT return types and operators with canonical versions
        RelNode rewritten = plan.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                return visited.accept(new DatetimeRexShuttle(rexBuilder, typeFactory));
            }
        });

        LOGGER.info("[DatetimeTypeRewriter] ═══ AFTER type replacement (before output cast) ═══\n{}", rewritten.explain());

        // Step 2: Add final Project with CAST(datetime -> VARCHAR) for output
        RelNode result = addOutputCastProject(rewritten, rexBuilder, typeFactory);

        LOGGER.info("[DatetimeTypeRewriter] ═══ AFTER output cast project ═══\n{}", result.explain());

        return result;
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
            LOGGER.info("[DatetimeTypeRewriter] No datetime fields in output — skipping output cast project");
            return plan;
        }

        LOGGER.info("[DatetimeTypeRewriter] Adding CAST-to-VARCHAR for {} datetime output field(s)", fieldNames.size());
        return LogicalProject.create(plan, List.of(), projects, fieldNames);
    }

    private static class DatetimeRexShuttle extends RexShuttle {
        private final RexBuilder rexBuilder;
        private final RelDataTypeFactory typeFactory;

        DatetimeRexShuttle(RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            this.rexBuilder = rexBuilder;
            this.typeFactory = typeFactory;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            RexCall visited = (RexCall) super.visitCall(call);

            String opName = visited.getOperator().getName().toUpperCase();
            SqlFunction canonicalOp = CANONICAL_OPS.get(opName);
            if (canonicalOp != null) {
                SqlTypeName targetType = standardReturnType(opName);
                if (targetType != null) {
                    RelDataType newType = typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(targetType),
                        visited.getType().isNullable()
                    );
                    LOGGER.info("[DatetimeTypeRewriter] Rewriting {}({}) : {} → {}",
                        opName, visited.getOperands(), visited.getType(), newType);
                    // Replace both operator (for Sig matching) and return type (for Substrait type)
                    return rexBuilder.makeCall(newType, canonicalOp, visited.getOperands());
                }
            }
            return visited;
        }
    }
}
