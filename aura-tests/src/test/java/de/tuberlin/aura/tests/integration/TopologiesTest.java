package de.tuberlin.aura.tests.integration;

import java.util.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.generator.TopologyGenerator;
import de.tuberlin.aura.core.dataflow.api.DataflowAPI;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.*;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.tests.util.TestHelper;


public final class TopologiesTest {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TopologiesTest.class);

    private static AuraClient auraClient;

    private static LocalClusterSimulator clusterSimulator = null;

    private static int executionUnits;

    // --------------------------------------------------
    // TESTS
    // --------------------------------------------------

    @BeforeClass
    public static void setup() {
        IConfig simConfig = IConfigFactory.load(IConfig.Type.SIMULATOR);
        switch (simConfig.getString("simulator.mode")) {
            case "LOCAL":
                new LocalClusterSimulator(simConfig);
                break;
            case "cluster":
                break;
            default:
                LOG.warn("'simulator mode' has unknown value. Fallback to LOCAL mode.");
                new LocalClusterSimulator(simConfig);
        }

        auraClient = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        int nodes = simConfig.getInt("simulator.tm.number");
        int cores = simConfig.getInt("tm.execution.units.number");

        executionUnits = nodes * cores;

        if (executionUnits < 8) {
            throw new IllegalStateException("TopologiesTest requires at least 8 execution units.");
        }

        LOG.info("start test with: " + nodes + " nodes and " + cores + " cores per node");
    }

    @Test
    public void testMinimalPlainTopology() {
        TestHelper.runTopology(auraClient, ExampleTopologies.two_layer_point2point_small(auraClient, executionUnits));
    }

    @Test
    public void testExtendedTopology() {
        TestHelper.runTopology(auraClient, ExampleTopologies.six_layer_all2all(auraClient, executionUnits));
    }

    @Test
    public void testPlainTopologiesConcurrently() {
        List<Topology.AuraTopology> topologies = new ArrayList<>();
        topologies.add(ExampleTopologies.two_layer_point2point_small(auraClient, executionUnits / 2));
        topologies.add(ExampleTopologies.two_layer_point2point_small(auraClient, executionUnits / 2));
        TestHelper.runTopologiesConcurrently(auraClient, topologies);
    }

    @Test
    public void testMultipleQueriesSequentially() {
        List<Topology.AuraTopology> topologies = new ArrayList<>();

        // 2 layered - all2all connection
        topologies.add(ExampleTopologies.two_layer_point2point_small(auraClient, executionUnits));

        // 3 layered - point2point + point2point connection
        topologies.add(ExampleTopologies.three_layer_point2point(auraClient, executionUnits));

        // 3 layered - all2all + point2point connection
        topologies.add(ExampleTopologies.three_layer_all2all_point2point(auraClient, executionUnits));

        // 3 layered - point2point + all2all connection
        topologies.add(ExampleTopologies.three_layer_point2point_all2all(auraClient, executionUnits));

        // 3 layered - all2all + all2all connection
        topologies.add(ExampleTopologies.three_layer_all2all_all2all(auraClient, executionUnits));

        // 3 layered - point2point (join) point2point connection
        topologies.add(ExampleTopologies.three_layer_point2point_join_point2point(auraClient, executionUnits));

        // 3 layered - all2all (join) point2point connection
        topologies.add(ExampleTopologies.three_layer_all2all_join_point2point(auraClient, executionUnits));

        // 3 layered - all2all (join) all2all connection
        topologies.add(ExampleTopologies.three_layer_all2all_join_all2all(auraClient, executionUnits));

        // 3 layered - all2all (join) all2all connection (small/large)
        topologies.add(ExampleTopologies.three_layer_all2all_join_all2all_sl(auraClient, executionUnits));

        TestHelper.runTopologies(auraClient, topologies);
    }

    @Test
    public void testOperatorTopology1() {
        final Topology.AuraTopology topology1 = testJob1(auraClient, executionUnits);
        TestHelper.runTopology(auraClient, topology1);
    }

    @Test
    public void testOperatorTopology2() {
        final Topology.AuraTopology topology2 = testJob2(auraClient, executionUnits);
        TestHelper.runTopology(auraClient, topology2);
    }

    @Test
    public void testOperatorTopology3() {
        final Topology.AuraTopology topology3 = testJob3(auraClient, executionUnits);
        TestHelper.runTopology(auraClient, topology3);
    }

    @Test
    public void testOperatorTopology4() {
        final Topology.AuraTopology topology4 = testJob4(auraClient, executionUnits);
        TestHelper.runTopology(auraClient, topology4);
    }

    @AfterClass
    public static void tearDown() {

        if (clusterSimulator != null) {
            clusterSimulator.shutdown();
        }

        auraClient.closeSession();
    }

    public static Topology.AuraTopology testJob1(AuraClient ac, int executionUnits) {

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor source1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source1", executionUnits / 6, 1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                null,
                                null,
                                source1TypeInfo,
                                Source1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        )
                );

        final DataflowAPI.DataflowNodeDescriptor map1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                                "Map1", executionUnits / 6, 1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                Map1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        source1
                );

        final DataflowAPI.DataflowNodeDescriptor flatMap1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.FLAT_MAP_TUPLE_OPERATOR,
                                "FlatMap1", executionUnits / 6, 1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                FlatMap1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        map1
                );

        final DataflowAPI.DataflowNodeDescriptor filter1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.FILTER_OPERATOR,
                                "Filter1", executionUnits / 6, 1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                Filter1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        flatMap1
                );

        final DataflowAPI.DataflowNodeDescriptor fold1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                                "Fold1", executionUnits / 6, 1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                Fold1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        filter1
                );

        final DataflowAPI.DataflowNodeDescriptor sink1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                                "Sink1", 1, 1,
                                null,
                                null,
                                source1TypeInfo,
                                null,
                                null,
                                Sink1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        fold1
                );

        return new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB1");
    }

    public static Topology.AuraTopology testJob2(AuraClient ac, int executionUnits) {

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor source1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source1", executionUnits / 8, 1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                null,
                                null,
                                source1TypeInfo,
                                Source1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        )
                );

        final DataflowAPI.DataflowNodeDescriptor source2 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source2", executionUnits / 8, 1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                null,
                                null,
                                source1TypeInfo,
                                Source2.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        )
                );

        final DataflowAPI.DataflowNodeDescriptor source3 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source3", executionUnits / 8, 1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                null,
                                null,
                                source1TypeInfo,
                                Source3.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        )
                );

        final DataflowAPI.DataflowNodeDescriptor difference1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.DIFFERENCE_OPERATOR,
                                "Difference1", executionUnits / 8, 1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                source1TypeInfo,
                                source1TypeInfo,
                                null,
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        source2,
                        source3
                );

        final TypeInformation join1TypeInfo =
                new TypeInformation(Tuple2.class,
                        source1TypeInfo,
                        source1TypeInfo);

        final DataflowAPI.DataflowNodeDescriptor join1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.HASH_JOIN_OPERATOR,
                                "Join1", executionUnits / 8, 1,
                                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                source1TypeInfo,
                                join1TypeInfo,
                                null,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") }, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") }, null, null, null,
                                null, null,
                                null
                        ),
                        source1,
                        difference1
                );


        final DataflowAPI.DataflowNodeDescriptor distinct1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.DISTINCT_OPERATOR,
                                "Distinct1", executionUnits / 8, 1,
                                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                join1TypeInfo,
                                null,
                                join1TypeInfo,
                                null,
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        join1
                );

        final DataflowAPI.DataflowNodeDescriptor sort1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                                "Sort1", executionUnits / 8, 1,
                                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                join1TypeInfo,
                                null,
                                join1TypeInfo,
                                null,
                                null, null, new int[][] { join1TypeInfo.buildFieldSelectorChain("_2._2") }, DataflowNodeProperties.SortOrder.DESCENDING, null,
                                null, null,
                                null
                        ),
                        distinct1
                );

        final DataflowAPI.DataflowNodeDescriptor sink1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                                "Sink1", 1, 1,
                                null,
                                null,
                                join1TypeInfo,
                                null,
                                null,
                                JoinSink1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        sort1
                );

        return new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB2");
    }

    public static Topology.AuraTopology testJob3(AuraClient ac, int executionUnits) {

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor source4 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source4", executionUnits / 5, 1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                null,
                                null,
                                source1TypeInfo,
                                Source4.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        )
                );

        // the optimizer will insert Sorts before GroupBys (using the same keys for sorting as for grouping)
        final DataflowAPI.DataflowNodeDescriptor sort1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                                "Sort1", executionUnits / 5, 1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                null,
                                null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") }, DataflowNodeProperties.SortOrder.ASCENDING, null,
                                null, null,
                                null
                        ),
                        source4
                );

        final TypeInformation groupBy1TypeInfo =
                new TypeInformation(Tuple2.class, true,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor groupBy1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                                "GroupBy1", executionUnits / 5, 1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                null,
                                groupBy1TypeInfo,
                                null,
                                null, null, null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                null, null,
                                null
                        ),
                        sort1
                );

        final DataflowAPI.DataflowNodeDescriptor fold1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                                "Fold1", executionUnits / 5, 1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                groupBy1TypeInfo,
                                null,
                                source1TypeInfo,
                                Fold1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        groupBy1
                );

        final DataflowAPI.DataflowNodeDescriptor sink1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                                "Sink1", 1, 1,
                                null,
                                null,
                                source1TypeInfo,
                                null,
                                null,
                                Sink1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        fold1
                );

        return new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB3");
    }

    public static Topology.AuraTopology testJob4(AuraClient ac, int executionUnits) {

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor source4 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source4", executionUnits / 6, 1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                null,
                                null,
                                source1TypeInfo,
                                Source4.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        )
                );

        // the optimizer will insert Sorts before GroupBys (using the same keys for sorting as for grouping)
        final DataflowAPI.DataflowNodeDescriptor sort1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                                "Sort1", executionUnits / 6, 1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                null,
                                null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") }, DataflowNodeProperties.SortOrder.ASCENDING, null,
                                null, null,
                                null
                        ),
                        source4
                );

        final TypeInformation groupBy1TypeInfo =
                new TypeInformation(Tuple2.class, true,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor groupBy1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                                "GroupBy1", executionUnits / 6, 1,
                                null,
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                null,
                                groupBy1TypeInfo,
                                null,
                                null, null, null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                null, null,
                                null
                        ),
                        sort1
                );

        final DataflowAPI.DataflowNodeDescriptor mapGroup1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.MAP_GROUP_OPERATOR,
                                "MapGroup1", executionUnits / 6, 1,
                                null,
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                groupBy1TypeInfo,
                                null,
                                groupBy1TypeInfo,
                                GroupMap1.class.getName(),
                                null, null, null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                null, null,
                                null
                        ),
                        groupBy1
                );

        final DataflowAPI.DataflowNodeDescriptor fold1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                                "Fold1", executionUnits / 6, 1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                groupBy1TypeInfo,
                                null,
                                source1TypeInfo,
                                Fold1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        mapGroup1
                );

        final DataflowAPI.DataflowNodeDescriptor sink1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                                "Sink1", 1, 1,
                                null,
                                null,
                                source1TypeInfo,
                                null,
                                null,
                                Sink1.class.getName(),
                                null, null, null, null, null,
                                null, null,
                                null
                        ),
                        fold1
                );

        return new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB4");
    }

    // ---------------------------------------------------
    // User-defined Functions.
    // ---------------------------------------------------

    public static final class Source1 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1200000;

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE1", count) : null;
        }
    }

    public static final class Source2 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 100000;

        Random rand = new Random(13454);

        @Override
        public Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("RIGHT_SOURCE", rand.nextInt(10000)) : null;
        }
    }

    public static final class Source3 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1000;

        Random rand = new Random(54321);

        @Override
        public Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("RIGHT_SOURCE", rand.nextInt(10000)) : null;
        }
    }

    public static final class Source4 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 100000;

        Random rand = new Random(13454);

        @Override
        public Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("RIGHT_SOURCE", rand.nextInt(100)) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            return new Tuple2<>("HELLO", in._2);
        }
    }

    public static final class FlatMap1 extends FlatMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public void flatMap(Tuple2<String,Integer> in, Collection<Tuple2<String,Integer>> c) {
            if ((in._2 % 10) == 0) {
                c.add(new Tuple2<>("HEL", in._2));
                c.add(new Tuple2<>("LO", in._2 + 1));
            }
        }

    }

    public static final class GroupMap1 extends GroupMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public void map(Iterator<Tuple2<String,Integer>> in, Collection<Tuple2<String,Integer>> output) {

            Integer count = 0;

            while (in.hasNext()) {
                Tuple2<String,Integer> t = in.next();
                output.add(new Tuple2<>(t._1, t._2 + count++));
            }
        }

    }

    public static final class Fold1 extends FoldFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> initialValue() {
            return new Tuple2<>("RESULT", 0);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> in) {
            return new Tuple2<>(in._1, 1);
        }

        @Override
        public Tuple2<String,Integer> add(Tuple2<String,Integer> currentValue, Tuple2<String, Integer> in) {
            return new Tuple2<>("RESULT", currentValue._2 + in._2);
        }
    }

    public static final class Filter1 extends FilterFunction<Tuple2<String,Integer>> {

        @Override
        public boolean filter(final Tuple2<String,Integer> in) {
            return in._2 % 2 == 0;
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {
//            System.out.println(in);
        }
    }

    public static final class JoinSink1 extends SinkFunction<Tuple2<Tuple2<String,Integer>,Tuple2<String,Integer>>> {

        @Override
        public void consume(final Tuple2<Tuple2<String,Integer>,Tuple2<String,Integer>> in) {
//            System.out.println(in);
        }
    }

}
