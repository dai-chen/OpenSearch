/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterDelegationInstructionNode;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.analytics.spi.ShardScanWithDelegationInstructionNode;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Planner-level evidence that a {@code fields id, name} projection over a <b>plain</b> index selects
 * the Lucene backend, and that the same projection over a <b>composite</b> index still selects the
 * value-producing (parquet/DataFusion) peer.
 *
 * <p>The discriminator is the per-field doc-value format {@code FieldStorageResolver} derives from
 * index settings: absent {@code index.composite.primary_data_format} it defaults to {@code lucene},
 * which is the only format Lucene's new {@link ScanCapability.DocValues} cap covers.
 */
public class PlainIndexScanSelectionTests extends OpenSearchTestCase {

    private static final IndexNameExpressionResolver TEST_RESOLVER = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
    }

    /** A plain index's fields report {@code docValueFormats=["lucene"]}, so Lucene must drive. */
    public void testPlainIndexProjectionSelectsLucene() {
        StagePlan plan = planProjection(null);
        assertEquals("plain-index projection must be driven by lucene", "lucene", plan.backendId());
    }

    /** A composite index's fields report {@code docValueFormats=["parquet"]}, so the peer must drive. */
    public void testCompositeIndexProjectionStillSelectsDataFusion() {
        StagePlan plan = planProjection("parquet");
        assertEquals("composite-index projection must stay on the value-producing peer", "mock-parquet", plan.backendId());
    }

    /** The fragment-shape gate must accept a plain-index projection and reject a composite one. */
    public void testValueScanFastPathGate() {
        assertTrue("plain index projection is a value-scan fast path", LuceneFragmentConvertor.isValueScanFastPath(markedProjection(null)));
        assertFalse(
            "composite index projection is not a value-scan fast path",
            LuceneFragmentConvertor.isValueScanFastPath(markedProjection("parquet"))
        );
    }

    /**
     * The scan's own viable-backend list must narrow to lucene for a plain index. The marked plan's
     * root is the coordinator gather ({@code OpenSearchExchangeReducer}), whose viable set is the
     * reduce-capable backends — the cross-backend split this PoC relies on — so assert on the scan.
     */
    public void testPlainIndexScanViableBackends() {
        RelNode marked = markedProjection(null);
        List<OpenSearchTableScan> scans = RelNodeUtils.findNodes(marked, OpenSearchTableScan.class);
        assertEquals("expected exactly one marked table scan in " + marked, 1, scans.size());
        assertEquals(List.of("lucene"), scans.getFirst().getViableBackends());

        // The gather above it must land on the reduce-capable peer, since Lucene has no exchange sink.
        List<OpenSearchExchangeReducer> reducers = RelNodeUtils.findNodes(marked, OpenSearchExchangeReducer.class);
        assertFalse("expected a coordinator gather in " + marked, reducers.isEmpty());
        assertEquals(List.of("mock-parquet"), reducers.getFirst().getViableBackends());
    }

    // ── Fixture ─────────────────────────────────────────────────────────────

    /**
     * Builds and marks {@code Project(id, name) over TableScan(plain_index)}, then runs the DAG
     * pipeline exactly as {@code DefaultPlanExecutor.executeInternal} does (fork → adapt → select)
     * and returns the leaf stage's surviving plan.
     *
     * @param primaryDataFormat value for {@code index.composite.primary_data_format}, or {@code null}
     *                          to omit the setting entirely (a plain index)
     */
    private StagePlan planProjection(String primaryDataFormat) {
        PlannerContext context = buildContext(primaryDataFormat);
        RelNode marked = PlannerImpl.runAllOptimizations(projection(), context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        BackendPlanAdapter.adaptAll(dag, context.getCapabilityRegistry());
        // preferMetadataDriver=true mirrors the production default (AnalyticsPlugin.PREFER_METADATA_DRIVER).
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), true);

        Stage leaf = dag.rootStage();
        while (leaf.getChildStages().isEmpty() == false) {
            leaf = leaf.getChildStages().getFirst();
        }
        List<StagePlan> alternatives = leaf.getPlanAlternatives();
        assertEquals("selector must collapse to a single alternative, got " + alternatives, 1, alternatives.size());
        return alternatives.getFirst();
    }

    private RelNode markedProjection(String primaryDataFormat) {
        return PlannerImpl.runAllOptimizations(projection(), buildContext(primaryDataFormat));
    }

    /** {@code Project(id, name)} over a two-column scan — the shape PPL's {@code fields} produces. */
    private LogicalProject projection() {
        RelOptTable table = mockTable("plain_index");
        TableScan scan = new TableScan(cluster, cluster.traitSet(), List.of(), table) {
        };
        List<RexNode> projects = new ArrayList<>();
        projects.add(rexBuilder.makeInputRef(scan, 0));
        projects.add(rexBuilder.makeInputRef(scan, 1));
        return LogicalProject.create(scan, List.of(), projects, List.of("id", "name"), Set.of());
    }

    private PlannerContext buildContext(String primaryDataFormat) {
        Map<String, Map<String, Object>> fields = new LinkedHashMap<>();
        fields.put("id", Map.of("type", "long"));
        fields.put("name", Map.of("type", "keyword"));

        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(Map.of("properties", fields));

        Settings.Builder settings = Settings.builder();
        if (primaryDataFormat != null) {
            // Composite index: parquet primary + lucene secondary, as CountFastPathIT provisions.
            settings.put("index.composite.primary_data_format", primaryDataFormat)
                .putList("index.composite.secondary_data_formats", "lucene");
        }

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("plain_index", "uuid"));
        when(indexMetadata.getSettings()).thenReturn(settings.build());
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(indexMetadata.getNumberOfShards()).thenReturn(2);

        Metadata metadata = mock(Metadata.class);
        when(metadata.index("plain_index")).thenReturn(indexMetadata);

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(metadata);

        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        CapabilityRegistry registry = new CapabilityRegistry(
            List.of(new StubDfBackend(), new LuceneAnalyticsBackendPlugin(null)),
            fieldStorageFactory
        );
        return new PlannerContext(registry, clusterState, null, false, true);
    }

    private RelOptTable mockTable(String tableName) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("id", typeFactory.createSqlType(SqlTypeName.BIGINT));
        builder.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of(tableName));
        when(table.getRowType()).thenReturn(builder.build());
        return table;
    }

    private ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        OperationRouting routing = mock(OperationRouting.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(routing);
        when(routing.searchShards(any(), any(), any(), any())).thenReturn(new GroupShardsIterator<ShardIterator>(List.of()));
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }

    /** Stands in for the DataFusion backend: value-producing doc values over {@code parquet} only. */
    private static class StubDfBackend implements AnalyticsSearchBackendPlugin {
        private static final Set<FieldType> TYPES = new HashSet<>();
        static {
            TYPES.addAll(FieldType.numeric());
            TYPES.addAll(FieldType.keyword());
            TYPES.addAll(FieldType.date());
            TYPES.add(FieldType.BOOLEAN);
        }

        @Override
        public String name() {
            return "mock-parquet";
        }

        @Override
        public BackendCapabilityProvider getCapabilityProvider() {
            return new BackendCapabilityProvider() {
                @Override
                public Set<EngineCapability> supportedEngineCapabilities() {
                    return Set.of(EngineCapability.SORT);
                }

                @Override
                public Set<ScanCapability> scanCapabilities() {
                    return Set.of(new ScanCapability.DocValues(Set.of("parquet"), TYPES));
                }

                @Override
                public Set<FilterCapability> filterCapabilities() {
                    Set<FilterCapability> caps = new HashSet<>();
                    for (ScalarFunction op : Set.of(
                        ScalarFunction.EQUALS,
                        ScalarFunction.NOT_EQUALS,
                        ScalarFunction.GREATER_THAN,
                        ScalarFunction.GREATER_THAN_OR_EQUAL,
                        ScalarFunction.LESS_THAN,
                        ScalarFunction.LESS_THAN_OR_EQUAL
                    )) {
                        caps.add(new FilterCapability.Standard(op, TYPES, Set.of("parquet")));
                    }
                    return caps;
                }

                @Override
                public Set<DelegationType> supportedDelegations() {
                    return Set.of(DelegationType.FILTER);
                }
            };
        }

        @Override
        public ExchangeSinkProvider getExchangeSinkProvider() {
            return (context, backendContext) -> null;
        }

        @Override
        public FragmentConvertor getFragmentConvertor() {
            return new FragmentConvertor() {
                @Override
                public byte[] convertFragment(RelNode fragment) {
                    return "fragment".getBytes(StandardCharsets.UTF_8);
                }

                @Override
                public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
                    return innerBytes;
                }

                @Override
                public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
                    return innerBytes;
                }
            };
        }

        @Override
        public FragmentInstructionHandlerFactory getInstructionHandlerFactory() {
            return new FragmentInstructionHandlerFactory() {
                @Override
                public Optional<InstructionNode> createShardScanNode(boolean requestsRowIds, String logicalTableName) {
                    return Optional.of(new ShardScanInstructionNode(requestsRowIds, logicalTableName));
                }

                @Override
                public Optional<InstructionNode> createFilterDelegationNode(
                    FilterTreeShape treeShape,
                    int delegatedPredicateCount,
                    List<DelegatedExpression> delegatedExpressions
                ) {
                    return Optional.of(new FilterDelegationInstructionNode(treeShape, delegatedPredicateCount, delegatedExpressions));
                }

                @Override
                public Optional<InstructionNode> createShardScanWithDelegationNode(
                    FilterTreeShape treeShape,
                    int delegatedPredicateCount,
                    boolean requestsRowIds,
                    String logicalTableName
                ) {
                    return Optional.of(
                        new ShardScanWithDelegationInstructionNode(treeShape, delegatedPredicateCount, requestsRowIds, logicalTableName)
                    );
                }

                @Override
                public Optional<InstructionNode> createPartialAggregateNode() {
                    return Optional.empty();
                }

                @Override
                public Optional<InstructionNode> createFinalAggregateNode() {
                    return Optional.empty();
                }

                @Override
                public FragmentInstructionHandler<?> createHandler(InstructionNode node) {
                    throw new UnsupportedOperationException("mock");
                }
            };
        }
    }
}
