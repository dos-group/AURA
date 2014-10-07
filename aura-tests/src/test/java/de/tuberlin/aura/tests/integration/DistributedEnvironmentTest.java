package de.tuberlin.aura.tests.integration;

import java.util.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;

public final class DistributedEnvironmentTest {

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
    public void testDataSetAsSink() {

        int dop = executionUnits / 3;

        final DataflowNodeProperties source1 = source1NodeProperties(dop);

        final DataflowNodeProperties map1 = map1NodeProperties(dop);

        final UUID dataset1UID = UUID.randomUUID();

        final DataflowNodeProperties dataset1Properties = dataset1NodeProperties(dataset1UID);

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source1), Source1.class).
                connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map1), Map1.class).
                connectTo("Dataset1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.DatasetNode(dataset1Properties));

        final Topology.AuraTopology topology1 = atb.build("JOB1");
        auraClient.submitTopology(topology1, null);
        auraClient.awaitSubmissionResult(1);

    }

    @Test
    public void testDataSetGenerationAndClientTransfer() {

        int dop = executionUnits / 3;

        final DataflowNodeProperties source1 = source1NodeProperties(dop);

        final DataflowNodeProperties map1 = map1NodeProperties(dop);

        final UUID dataset1UID = UUID.randomUUID();

        final DataflowNodeProperties dataset1Properties = dataset1NodeProperties(dataset1UID);

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source1), Source1.class).
                connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map1), Map1.class).
                connectTo("Dataset1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.DatasetNode(dataset1Properties));

        final Topology.AuraTopology topology1 = atb.build("JOB1");
        auraClient.submitTopology(topology1, null);
        auraClient.awaitSubmissionResult(1);

        final Collection<Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>>> collection1 = auraClient.getDataset(dataset1UID);
//        for (Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>> tuple : collection1)
//            System.out.println(tuple);

    }

    @Test
    public void testDataSetAsSinkAndSource() {

        int dop = executionUnits / 3;

        final DataflowNodeProperties source1 = source1NodeProperties(dop);

        final DataflowNodeProperties map1 = map1NodeProperties(dop);

        final UUID dataset1UID = UUID.randomUUID();

        final DataflowNodeProperties dataset1Properties = dataset1NodeProperties(dataset1UID);

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source1), Source1.class).
                connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map1), Map1.class).
                connectTo("Dataset1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.DatasetNode(dataset1Properties));

        final Topology.AuraTopology topology1 = atb.build("JOB1");
        auraClient.submitTopology(topology1, null);
        auraClient.awaitSubmissionResult(1);

        Collection<Tuple2<String,Integer>> broadcastDataset = new ArrayList<>();
        broadcastDataset.add(new Tuple2<>("A", 0));
        broadcastDataset.add(new Tuple2<>("B", 1));
        broadcastDataset.add(new Tuple2<>("C", 2));
        broadcastDataset.add(new Tuple2<>("D", 3));
        final UUID broadcastDatasetID = UUID.randomUUID();
        auraClient.broadcastDataset(broadcastDatasetID, broadcastDataset);

        final DataflowNodeProperties map2 = map2NodeProperties(dop, broadcastDatasetID);

        final UUID dataset2UID = UUID.randomUUID();

        final DataflowNodeProperties dataset2Properties = dataset2NodeProperties(dataset2UID);

        Topology.AuraTopologyBuilder atb2 = auraClient.createTopologyBuilder();

        atb2.addNode(new Topology.DatasetNode(dataset1Properties)).
                connectTo("Map2", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map2), Map2.class).
                connectTo("Dataset2", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.DatasetNode(dataset2Properties));

        final Topology.AuraTopology topology2 = atb2.build("JOB2");
        auraClient.submitTopology(topology2, null);
        auraClient.awaitSubmissionResult(1);

    }

    @AfterClass
    public static void tearDown() {

        if (!IntegrationTestSuite.isRunning) {
            IntegrationTestSuite.tearDownTestEnvironment();
        }

        auraClient.closeSession();
    }



    private DataflowNodeProperties map2NodeProperties(int dop, UUID broadcastDatasetID) {

        TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map2",
                dop,
                1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                Map2.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                Arrays.asList(broadcastDatasetID)
        );
    }

    private DataflowNodeProperties source1NodeProperties(int dop) {

        TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source1",
                dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Source1.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private DataflowNodeProperties map1NodeProperties(int dop) {

        TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map1",
                dop,
                1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                Map1.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private DataflowNodeProperties dataset2NodeProperties(UUID dataset2UID) {

        TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                dataset2UID,
                DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                "Dataset2",
                1, // executionUnits / 3,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private DataflowNodeProperties dataset1NodeProperties(UUID dataset1UID) {

        TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                dataset1UID,
                DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                "Dataset1",
                1, // dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private TypeInformation source1TypeInfo() {
        return new TypeInformation(Tuple2.class,
                new TypeInformation(String.class),
                new TypeInformation(Integer.class));
    }

    // ---------------------------------------------------
    // User-defined Functions.
    // ---------------------------------------------------

    public static final class Source1 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1000;

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE1", count) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            return new Tuple2<>("HELLO", in._2);
        }
    }

    public static final class Map2 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        private Collection<Tuple2<String,Integer>> broadcastDataset;

        @Override
        public void create() {
            final UUID dataset1 = getEnvironment().getProperties().broadcastVars.get(0);
            broadcastDataset = getEnvironment().getDataset(dataset1);
        }

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            final StringBuilder sb = new StringBuilder();
            for(final Tuple2<String,Integer> t : broadcastDataset)
                sb.append(t._1);
            return new Tuple2<>("HELLO" + sb.toString(), in._2);
        }
    }


}
