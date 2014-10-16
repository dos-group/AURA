package de.tuberlin.aura.tests.integration;

import java.util.*;

import de.tuberlin.aura.core.record.tuples.Tuple3;
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

    // TODO: a test for the Group Map

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
    public void testJob1MapFlatMapFilterDataflow() {

        int dop = executionUnits / 6;

        final TypeInformation source1TypeInfo = source1TypeInfo();

        Topology.OperatorNode sourceNode =
                new Topology.OperatorNode(
                    new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                        "Source",
                        dop,
                        1,
                        null,
                        null,
                        null,
                        null,
                        source1TypeInfo,
                        Job1Source.class.getName(),
                        null, null, null, null, null,
                        null, null, null
                    )
                );

        Topology.OperatorNode mapNode =
                new Topology.OperatorNode(
                    new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                        "Map", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        Job1Map.class.getName(),
                        null, null, null, null, null,
                        null, null, null
                    )
                );

        Topology.OperatorNode flatMapNode =
                new Topology.OperatorNode(
                    new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.FLAT_MAP_TUPLE_OPERATOR,
                        "FlatMap", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        Job1FlatMap.class.getName(),
                        null, null, null, null, null,
                        null, null, null
                    )
                );

        Topology.OperatorNode  filterNode =
                new Topology.OperatorNode(
                    new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.FILTER_OPERATOR,
                        "Filter", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        Job1Filter.class.getName(),
                        null, null, null, null, null,
                        null, null, null
                    )
                );

        Topology.OperatorNode sinkNode =
                new Topology.OperatorNode(
                    new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                        "Sink", 1, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        null,
                        Job1Sink.class.getName(),
                        null, null, null, null, null,
                        null, null, null
                    )
                );

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(sourceNode).
            connectTo("Map", Topology.Edge.TransferType.POINT_TO_POINT).
            addNode(mapNode).
            connectTo("FlatMap", Topology.Edge.TransferType.POINT_TO_POINT).
            addNode(flatMapNode).
            connectTo("Filter", Topology.Edge.TransferType.POINT_TO_POINT).
            addNode(filterNode).
            connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT).
            addNode(sinkNode);

        final Topology.AuraTopology topology1 = atb.build("JOB1-Maps");

        TestHelper.runTopology(auraClient, topology1);
    }

    @Test
    public void testJob2JoinDataflow() {

        int dop = executionUnits / 7;

        final TypeInformation source1TypeInfo = source1TypeInfo();

        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        final TypeInformation join2TypeInfo = join1TypeInfo(join1TypeInfo, source1TypeInfo());

        Topology.OperatorNode source1Node =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source1",
                                dop,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                null,
                                null,
                                source1TypeInfo,
                                Job2Source.class.getName(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null, null
                        ));

        Topology.OperatorNode source2Node =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
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
                                Job2Source.class.getName(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null, null
                        ));

        Topology.OperatorNode mapNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                                "Map",
                                dop,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                Job2Map.class.getName(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null, null
                        ));

        Topology.OperatorNode join1Node =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
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
                                null, null
                        ));

        Topology.OperatorNode join2Node =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.HASH_JOIN_OPERATOR,
                                "Join2",
                                dop,
                                1,
                                new int[][] { join2TypeInfo.buildFieldSelectorChain("_1._2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                join1TypeInfo,
                                source1TypeInfo,
                                join2TypeInfo,
                                null,
                                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1") },
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                null,
                                null,
                                null,
                                null,
                                null, null
                        ));

        Topology.OperatorNode sinkNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                                "Sink",
                                dop,
                                1,
                                null,
                                null,
                                join2TypeInfo,
                                null,
                                null,
                                Job2Sink.class.getName(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null, null
                        ));

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(source1Node)
            .connectTo("Map", Topology.Edge.TransferType.POINT_TO_POINT)
            .addNode(mapNode)
            .connectTo("Join1", Topology.Edge.TransferType.POINT_TO_POINT)
            .addNode(source2Node)
            .connectTo("Join2", Topology.Edge.TransferType.POINT_TO_POINT)
            .and().connectTo("Join1", Topology.Edge.TransferType.POINT_TO_POINT)
            .addNode(join1Node)
            .connectTo("Join2", Topology.Edge.TransferType.POINT_TO_POINT)
            .addNode(join2Node)
            .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
            .addNode(sinkNode);

        final Topology.AuraTopology topology = atb.build("JOB2-Joins");

        TestHelper.runTopology(auraClient, topology);
    }

    @Test
    public void testJob3DistinctUnionDataflow() {

        int dop = executionUnits / 5;

        final TypeInformation source1TypeInfo = source1TypeInfo();

        Topology.OperatorNode source1Node =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source1",
                                dop,
                                1,
                                null,
                                null,
                                null,
                                null,
                                source1TypeInfo,
                                Job3Source1.class.getName(),
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.OperatorNode source2Node =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source2",
                                dop,
                                1,
                                null,
                                null,
                                null,
                                null,
                                source1TypeInfo,
                                Job3Source2.class.getName(),
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.OperatorNode unionNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UNION_OPERATOR,
                                "Union",
                                dop,
                                1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_1"), source1TypeInfo.buildFieldSelectorChain("_2")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                source1TypeInfo,
                                source1TypeInfo,
                                null,
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.OperatorNode distinctNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.DISTINCT_OPERATOR,
                                "Distinct",
                                dop,
                                1,
                                null,
                                null,
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                null,
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.OperatorNode sinkNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                                "Sink", 1, 1,
                                null,
                                null,
                                source1TypeInfo,
                                null,
                                null,
                                Job3Sink.class.getName(),
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(source1Node).
            connectTo("Union", Topology.Edge.TransferType.POINT_TO_POINT).
            addNode(source2Node).
            connectTo("Union", Topology.Edge.TransferType.POINT_TO_POINT).
            addNode(unionNode).
            connectTo("Distinct", Topology.Edge.TransferType.ALL_TO_ALL).
            addNode(distinctNode).
            connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT).
            addNode(sinkNode);

        final Topology.AuraTopology topology1 = atb.build("JOB3-DistinctUnion");

        TestHelper.runTopology(auraClient, topology1);
    }

    @Test
    public void testJob4DifferenceDataflow() {

        int dop = executionUnits / 4;

        final TypeInformation source1TypeInfo = source1TypeInfo();

        Topology.OperatorNode source1Node =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source1",
                                dop,
                                1,
                                null,
                                null,
                                null,
                                null,
                                source1TypeInfo,
                                Job4Source1.class.getName(),
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.OperatorNode source2Node =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source2",
                                dop,
                                1,
                                null,
                                null,
                                null,
                                null,
                                source1TypeInfo,
                                Job4Source2.class.getName(),
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.OperatorNode differenceNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.DIFFERENCE_OPERATOR,
                                "Difference",
                                dop,
                                1,
                                null,
                                null,
                                source1TypeInfo,
                                source1TypeInfo,
                                source1TypeInfo,
                                null,
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.OperatorNode sinkNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                                "Sink", 1, 1,
                                null,
                                null,
                                source1TypeInfo,
                                null,
                                null,
                                Job4Sink.class.getName(),
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(source1Node).
                connectTo("Difference", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(source2Node).
                connectTo("Difference", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(differenceNode).
                connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(sinkNode);

        final Topology.AuraTopology topology1 = atb.build("JOB4-Difference");

        TestHelper.runTopology(auraClient, topology1);
    }

    @Test
    public void testJob5GlobalFold() {

        int dop = executionUnits / 3;

        final TypeInformation source1TypeInfo = source1TypeInfo();

        Topology.OperatorNode sourceNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source",
                                dop,
                                1,
                                null,
                                null,
                                null,
                                null,
                                source1TypeInfo,
                                Job5Source.class.getName(),
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.OperatorNode foldNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                                "Fold",
                                dop,
                                1,
                                null,
                                null,
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                Job5Fold.class.getName(),
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.OperatorNode sinkNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                                "Sink", dop, 1,
                                null,
                                null,
                                source1TypeInfo,
                                null,
                                null,
                                Job5Sink.class.getName(),
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(sourceNode).
                connectTo("Fold", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(foldNode).
                connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(sinkNode);

        final Topology.AuraTopology topology1 = atb.build("JOB5-GlobalFold");

        TestHelper.runTopology(auraClient, topology1);
    }

    @Test
    public void testJob6ChainedFoldWithShufflingDataflow() {

        int dop = executionUnits / 4;

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation groupBy1TypeInfo = groupBy1TypeInfo();

        Topology.OperatorNode sourceNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source", dop, 1,
                                null,
                                null,
                                null,
                                null,
                                source1TypeInfo,
                                Job6Source.class.getName(),
                                null, null, null, null, null,
                                null, null, null
                        )
                );

        DataflowNodeProperties sort1NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                        "Sort1", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        null,
                        null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        DataflowNodeProperties.SortOrder.ASCENDING, null,
                        null, null, null
                );

        DataflowNodeProperties groupBy1NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                        "GroupBy1", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        groupBy1TypeInfo,
                        null,
                        null, null, null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        null, null, null
                );

        DataflowNodeProperties fold1NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                        "Fold1", dop, 1,
                        new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        groupBy1TypeInfo,
                        null,
                        source1TypeInfo,
                        Job6Fold.class.getName(),
                        null, null, null, null, null,
                        null, null, null
                );

        Topology.OperatorNode chainedSortGroupFold1Node = new Topology.OperatorNode(Arrays.asList(sort1NodeProperties,
                groupBy1NodeProperties,
                fold1NodeProperties));

        DataflowNodeProperties sort2NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                        "Sort2", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        null,
                        null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        DataflowNodeProperties.SortOrder.ASCENDING, null,
                        null, null, null
                );

        DataflowNodeProperties groupBy2NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                        "GroupBy2", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        groupBy1TypeInfo,
                        null,
                        null, null, null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        null, null, null
                );

        DataflowNodeProperties fold2NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                        "Fold2", dop, 1,
                        null,
                        null,
                        groupBy1TypeInfo,
                        null,
                        source1TypeInfo,
                        Job6Fold.class.getName(),
                        null, null, null, null, null,
                        null, null, null
                );

        Topology.OperatorNode chainedSortGroupFold2Node = new Topology.OperatorNode(Arrays.asList(sort2NodeProperties,
                groupBy2NodeProperties,
                fold2NodeProperties));

        Topology.OperatorNode sinkNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                                "Sink", 1, 1,
                                null,
                                null,
                                source1TypeInfo,
                                null,
                                null,
                                Job6Sink.class.getName(),
                                null, null, null, null,
                                null, null, null, null
                        )
                );

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        atb.addNode(sourceNode).
                connectTo("Sort1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(chainedSortGroupFold1Node).
                connectTo("Sort2", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(chainedSortGroupFold2Node).
                connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(sinkNode);

        final Topology.AuraTopology topology = atb.build("JOB5-ChainedGroupFolding");

        TestHelper.runTopology(auraClient, topology);
    }

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
    // User-defined Functions.
    // ---------------------------------------------------

    public static final class Job1Source extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1200000;

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE1", count) : null;
        }
    }

    public static final class Job1Map extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            return new Tuple2<>("HELLO", in._2);
        }
    }

    public static final class Job1FlatMap extends FlatMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public void flatMap(Tuple2<String,Integer> in, Collection<Tuple2<String,Integer>> c) {
            if ((in._2 % 10) == 0) {
                c.add(new Tuple2<>("HEL", in._2));
                c.add(new Tuple2<>("LO", in._2 + 1));
            }
        }

    }

    public static final class Job1Filter extends FilterFunction<Tuple2<String,Integer>> {

        @Override
        public boolean filter(final Tuple2<String,Integer> in) {
            return in._2 % 2 == 0;
        }
    }

    public static final class Job1Sink extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {
//            System.out.println(in);
        }
    }

    public static final class Job2Source extends SourceFunction<Tuple2<Integer, String>> {

        int count = 100000;

        @Override
        public Tuple2<Integer, String> produce() {
            return (--count >= 0 ) ?  new Tuple2<>(count, "String" + count) : null;
        }
    }

    public static final class Job2Map extends MapFunction<Tuple2<Integer, String>, Tuple2<Integer,String>> {

        @Override
        public Tuple2<Integer,String> map(final Tuple2<Integer, String> in) {
            return in;
        }
    }

    public static final class Job2Sink extends SinkFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>> {

        @Override
        public void consume(final Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> in) {
//            System.out.println(in);
        }
    }

    public static final class Job3Source1 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1200000;

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE1", 1) : null;
        }
    }

    public static final class Job3Source2 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1200000;

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE2", 2) : null;
        }
    }

    public static final class Job3Sink extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {
//            System.out.println(in);
        }
    }

    public static final class Job4Source1 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 12000;

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("Bam Bam Bam..", count) : null;
        }
    }

    public static final class Job4Source2 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 11975;

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("Bam Bam Bam..", count) : null;
        }
    }

    public static final class Job4Sink extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {
//            System.out.println(in);
        }
    }

    public static final class Job5Source extends SourceFunction<Tuple3<String,Integer,Integer>> {

        int count = 10000;

        @Override
        public  Tuple3<String,Integer,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple3<>(String.valueOf(count), count, 1) : null;
        }
    }

    public static final class Job5Fold extends FoldFunction<Tuple3<String,Integer,Integer>,Tuple2<String,Integer>> {


        @Override
        public Tuple2<String, Integer> empty() {
            return new Tuple2<>("RESULT", 0);
        }

        @Override
        public Tuple2<String, Integer> singleton(Tuple3<String, Integer, Integer> element) {
            return new Tuple2<>(element._1, element._3);
        }

        @Override
        public Tuple2<String, Integer> union(Tuple2<String, Integer> result, Tuple2<String, Integer> element) {
            result._2 = result._2 + element._2;
            return result;
        }
    }

    public static final class Job5Sink extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {
//            System.out.println(in);
        }
    }

    public static final class Job6Source extends SourceFunction<Tuple2<String,Integer>> {

        int count = 100000;

        @Override
        public Tuple2<String,Integer> produce() {

            --count;

            if (count >= 75000) {
                return new Tuple2<>("First_Group", 1);
            } else if (count >= 50000) {
                return new Tuple2<>("Second_Group", 1);
            } else if (count >= 25000) {
                return new Tuple2<>("Third_Group", 1);
            } else if (count >= 0) {
                return new Tuple2<>("Last_Group", 1);
            } else {
                return null;
            }
        }
    }

    public static final class Job6Fold extends FoldFunction<Tuple2<String,Integer>,Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> empty() {
            return new Tuple2<>("", 0);
        }

        @Override
        public Tuple2<String, Integer> singleton(Tuple2<String, Integer> element) {
            return element;
        }

        @Override
        public Tuple2<String,Integer> union(Tuple2<String, Integer> result, Tuple2<String, Integer> element) {
            result._1 = element._1; // group key
            result._2 = result._2 + element._2;
            return result;
        }
    }

    public static final class Job6Sink extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {
//            System.out.println(in);
        }
    }

}
