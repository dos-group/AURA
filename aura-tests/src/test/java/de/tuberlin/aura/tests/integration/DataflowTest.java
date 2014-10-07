package de.tuberlin.aura.tests.integration;

import java.util.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.*;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.tests.util.TestHelper;

public final class DataflowTest {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static AuraClient auraClient;

    private static int executionUnits;

    // --------------------------------------------------
    // Tests.
    // --------------------------------------------------

    @BeforeClass
    public static void setup() {

        IConfig simConfig = IConfigFactory.load(IConfig.Type.SIMULATOR);

        if (!IntegrationTestSuite.isRunning)
            IntegrationTestSuite.setUpTestEnvironment();

        auraClient = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        executionUnits = simConfig.getInt("simulator.tm.number") * simConfig.getInt("tm.execution.units.number");

    }

    @Test
    public void testDataflow1() {

        int dop = executionUnits / 6;

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.OperatorNode(source1NodeProperties(dop)), Source1.class).
                connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map1NodeProperties(dop)), Map1.class).
                connectTo("FlatMap1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(flatMap1NodeProperties(dop)), FlatMap1.class).
                connectTo("Filter1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(filter1NodeProperties(dop)), Filter1.class).
                connectTo("Fold1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(fold1NodeProperties(dop)), Fold1.class).
                connectTo("Sink1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(sink1NodeProperties()), Sink1.class);

        final Topology.AuraTopology topology1 = atb.build("JOB1");

        TestHelper.runTopology(auraClient, topology1);
    }

    @Test
    public void testDataflow2() {

        if (executionUnits < 8) {
            throw new IllegalStateException("DataflowTest requires at least 8 execution units.");
        }

        int dop = executionUnits / 8;

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source1NodeProperties(dop)), Source1.class).
                connectTo("Join1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(source2NodeProperties(dop)), Source2.class).
                connectTo("Difference1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(source3NodeProperties(dop)), Source3.class).
                connectTo("Difference1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(difference1NodeProperties(dop))).
                connectTo("Join1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(join1NodeProperties(dop))).
                connectTo("Distinct1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(distinct1NodeProperties(dop))).
                connectTo("Sort1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(sort1JoinTypesNodeProperties(dop))).
                connectTo("Sink2", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(sink2NodeProperties()), JoinSink1.class);

        final Topology.AuraTopology topology2 = atb.build("JOB2");

        TestHelper.runTopology(auraClient, topology2);
    }

    @Test
    public void testDataflow3() {

        int dop = executionUnits / 5;

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source4NodeProperties(dop)), Source4.class).
                connectTo("Sort1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(sort1SourceTypeNodeProperties(dop))).
                connectTo("GroupBy1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(groupBy1NodeProperties(dop))).
                connectTo("Fold1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(fold1GroupTypeNodeProperties(dop)), Fold1.class).
                connectTo("Sink1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(sink1NodeProperties()), Sink1.class);

        final Topology.AuraTopology topology3 = atb.build("JOB3");

        TestHelper.runTopology(auraClient, topology3);
    }

    @Test
    public void testDataflow4() {

        int dop = executionUnits / 6;

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source4NodeProperties(dop)), Source4.class).
                connectTo("Sort1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(sort1SourceTypeNodeProperties(dop))).
                connectTo("GroupBy1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(groupBy1NodeProperties(dop))).
                connectTo("MapGroup1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(mapGroup1NodeProperties(dop)), GroupMap1.class).
                connectTo("Fold1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(fold1GroupTypeNodeProperties(dop)), Fold1.class).
                connectTo("Sink1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(sink1NodeProperties()), Sink1.class);

        final Topology.AuraTopology topology4 = atb.build("JOB4");

        TestHelper.runTopology(auraClient, topology4);
    }

    @Test
    public void testDataflow5() {

        int dop = executionUnits / 7;

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.OperatorNode(sourceANodeProperties(dop)), SourceA.class)
                .connectTo("MapA", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(mapANodeProperties(dop)), MapA.class)
                .connectTo("Join1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(sourceBNodeProperties(dop)), SourceA.class)
                .connectTo("Join2", Topology.Edge.TransferType.POINT_TO_POINT)
                .and().connectTo("Join1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(joinANodeProperties(dop)), new ArrayList<Class<?>>())
                .connectTo("Join2", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(joinBNodeProperties(dop)), new ArrayList<Class<?>>())
                .connectTo("SinkA", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(sinkANodeProperties(dop)), SinkA.class);

        final Topology.AuraTopology topology5 = atb.build("JOB5");

        TestHelper.runTopology(auraClient, topology5);
    }

    /*@Test
    public void testOperatorTopology6() {

        int dop = executionUnits / ?;

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

                DataflowNodeProperties sink2 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                "Sink2",
                dop,
                1,
                null,
                null,
                source1TypeInfo,
                null,
                null,
                SinkA.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );


                Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();
        atb.addNode(new Topology.OperatorNode(sourceA), SourceA.class)
                .connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(map), Map1.class)
                .connectTo("SinkA", Topology.Edge.TransferType.POINT_TO_POINT)
                .and().connectTo("Sink2", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(sink1), SinkA.class)
                .noConnects()
                .addNode(new Topology.OperatorNode(sink2), SinkA.class);

        final Topology.AuraTopology topology4 = atb.build("JOB6");

        TestHelper.runTopology(auraClient, topology4);
    }*/

    @AfterClass
    public static void tearDown() {

        if (!IntegrationTestSuite.isRunning) {
            IntegrationTestSuite.tearDownTestEnvironment();
        }

        auraClient.closeSession();
    }

    // ---------------------------------------------------
    // Types.
    // ---------------------------------------------------

    private static TypeInformation source1TypeInfo() {
        return new TypeInformation(Tuple2.class,
                new TypeInformation(String.class),
                new TypeInformation(Integer.class));
    }

    private static TypeInformation join1TypeInfo(TypeInformation joinedType1, TypeInformation joinedType2) {
        return new TypeInformation(Tuple2.class,
                joinedType1,
                joinedType2);
    }

    private static TypeInformation groupBy1TypeInfo() {
        return new TypeInformation(Tuple2.class, true,
                new TypeInformation(String.class),
                new TypeInformation(Integer.class));
    }

    // ---------------------------------------------------
    // Dataflow Nodes.
    // ---------------------------------------------------

    private static DataflowNodeProperties source1NodeProperties(int source1dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source1",
                source1dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Source1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties source2NodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source2", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Source2.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties source3NodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source3", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Source3.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties source4NodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source4", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Source4.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private DataflowNodeProperties sourceANodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "SourceA",
                dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                SourceA.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private DataflowNodeProperties sourceBNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source2",
                dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                SourceA.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private static DataflowNodeProperties sink1NodeProperties() {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
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
                null, null
        );
    }

    private static DataflowNodeProperties sink2NodeProperties() {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                "Sink2", 1, 1,
                null,
                null,
                join1TypeInfo,
                null,
                null,
                JoinSink1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private DataflowNodeProperties sinkANodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        final TypeInformation join2TypeInfo = join1TypeInfo(join1TypeInfo, source1TypeInfo());

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                "SinkA",
                dop,
                1,
                null,
                null,
                join2TypeInfo,
                null,
                null,
                SinkA.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private static DataflowNodeProperties map1NodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map1", dop, 1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                Map1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private DataflowNodeProperties mapANodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "MapA",
                dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                MapA.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private static DataflowNodeProperties flatMap1NodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.FLAT_MAP_TUPLE_OPERATOR,
                "FlatMap1", dop, 1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                FlatMap1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties filter1NodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.FILTER_OPERATOR,
                "Filter1", dop, 1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                Filter1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties fold1NodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                "Fold1", dop, 1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                Fold1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties sort1JoinTypesNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                "Sort1", dop, 1,
                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                join1TypeInfo,
                null,
                join1TypeInfo,
                null,
                null, null, new int[][] { join1TypeInfo.buildFieldSelectorChain("_2._2") }, DataflowNodeProperties.SortOrder.DESCENDING, null,
                null, null
        );
    }

    private static DataflowNodeProperties distinct1NodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.DISTINCT_OPERATOR,
                "Distinct1", dop, 1,
                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                join1TypeInfo,
                null,
                join1TypeInfo,
                null,
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties join1NodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.HASH_JOIN_OPERATOR,
                "Join1", dop, 1,
                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                source1TypeInfo,
                join1TypeInfo,
                null,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") }, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") }, null, null, null,
                null, null
        );
    }

    private DataflowNodeProperties joinANodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.HASH_JOIN_OPERATOR,
                "Join1",
                dop,
                1,
                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                source1TypeInfo,
                join1TypeInfo,
                null,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                null,
                null,
                null,
                null,
                null
        );
    }

    private DataflowNodeProperties joinBNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        final TypeInformation join2TypeInfo = join1TypeInfo(join1TypeInfo, source1TypeInfo());

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.HASH_JOIN_OPERATOR,
                "Join2",
                dop,
                1,
                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                join1TypeInfo,
                source1TypeInfo,
                join2TypeInfo,
                null,
                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1") },
                new int[][] { join1TypeInfo.buildFieldSelectorChain("_2._1") },
                null,
                null,
                null,
                null,
                null
        );
    }

    private static DataflowNodeProperties difference1NodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.DIFFERENCE_OPERATOR,
                "Difference1", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                source1TypeInfo,
                source1TypeInfo,
                null,
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties groupBy1NodeProperties(int dop) {

        TypeInformation source1TypeInfo = source1TypeInfo();
        TypeInformation groupBy1TypeInfo = groupBy1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                "GroupBy1", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                groupBy1TypeInfo,
                null,
                null, null, null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                null, null
        );
    }

    private static DataflowNodeProperties fold1GroupTypeNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation groupBy1TypeInfo = groupBy1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                "Fold1", dop, 1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                groupBy1TypeInfo,
                null,
                source1TypeInfo,
                Fold1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties sort1SourceTypeNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                "Sort1", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                null,
                null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") }, DataflowNodeProperties.SortOrder.ASCENDING, null,
                null, null
        );
    }

    private static DataflowNodeProperties mapGroup1NodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation groupBy1TypeInfo = groupBy1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_GROUP_OPERATOR,
                "MapGroup1", dop, 1,
                null,
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                groupBy1TypeInfo,
                null,
                groupBy1TypeInfo,
                GroupMap1.class.getName(),
                null, null, null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                null, null
        );
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

    public static final class SourceA extends SourceFunction<Tuple2<Integer, String>> {

        int count = 100000;

        @Override
        public Tuple2<Integer, String> produce() {
            return (--count >= 0 ) ?  new Tuple2<>(count, "String" + count) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            return new Tuple2<>("HELLO", in._2);
        }
    }

    public static final class MapA extends MapFunction<Tuple2<Integer, String>, Tuple2<Integer,String>> {

        @Override
        public Tuple2<Integer,String> map(final Tuple2<Integer, String> in) {
            return in;
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

    public static final class SinkA extends SinkFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>> {

        @Override
        public void consume(final Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> in) {
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
