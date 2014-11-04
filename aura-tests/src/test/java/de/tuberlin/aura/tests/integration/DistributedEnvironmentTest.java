package de.tuberlin.aura.tests.integration;

import java.util.*;

import de.tuberlin.aura.core.taskmanager.TaskManagerStatus;
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
import de.tuberlin.aura.core.dataflow.operators.impl.DatasetUpdatePhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.UpdateFunction;
import de.tuberlin.aura.tests.util.TestHelper;

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
    public void testJob1DataSetSink() {

        int dop = executionUnits / 3;

        final DataflowNodeProperties source1 = source1NodeProperties(dop);

        final DataflowNodeProperties map1 = map1NodeProperties(dop);

        final UUID dataset1UID = UUID.randomUUID();

        final DataflowNodeProperties dataset1Properties = dataset1NodeProperties(dataset1UID, 1);

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source1)).
                connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map1)).
                connectTo("Dataset1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.DatasetNode(dataset1Properties));

        final Topology.AuraTopology topology1 = atb.build("JOB1");

        TestHelper.runTopology(auraClient, topology1);

        auraClient.eraseDataset(dataset1UID);
    }

    @Test
    public void testJob2DataSetGenerationAndClientTransfer() {

        int dop = executionUnits / 3;

        final DataflowNodeProperties source1 = source1NodeProperties(dop);

        final DataflowNodeProperties map1 = map1NodeProperties(dop);

        final UUID dataset1UID = UUID.randomUUID();

        final DataflowNodeProperties dataset1Properties = dataset1NodeProperties(dataset1UID, dop);

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source1)).
                connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map1)).
                connectTo("Dataset1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.DatasetNode(dataset1Properties));

        final Topology.AuraTopology topology1 = atb.build("testJob2DataSetGenerationAndClientTransfer");

        TestHelper.runTopology(auraClient, topology1);

        final Collection<Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>>> collection1 = auraClient.getDataset(dataset1UID);

        auraClient.eraseDataset(dataset1UID);

        for (Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>> tuple : collection1) {
            //  System.out.println(tuple);
        }
    }

    @Test
    public void testJob3GeneratedDataSetAsSourceForFollowingDataflow() {

        int dop = executionUnits / 3;

        final DataflowNodeProperties source1 = source1NodeProperties(dop);

        final DataflowNodeProperties map1 = map1NodeProperties(dop);

        final UUID dataset1UID = UUID.randomUUID();

        final DataflowNodeProperties dataset1Properties = dataset1NodeProperties(dataset1UID, dop);

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source1)).
                connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map1)).
                connectTo("Dataset1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.DatasetNode(dataset1Properties));

        final Topology.AuraTopology topology1 = atb.build("testJob3GeneratedDataSetAsSourceForFollowingDataflow-1");

        TestHelper.runTopology(auraClient, topology1);

        Collection<Tuple2<String,Integer>> broadcastDataset = new ArrayList<>();
        broadcastDataset.add(new Tuple2<>("A", 0));
        broadcastDataset.add(new Tuple2<>("B", 1));
        broadcastDataset.add(new Tuple2<>("C", 2));
        broadcastDataset.add(new Tuple2<>("D", 3));

        final UUID broadcastDatasetID = UUID.randomUUID();
        auraClient.broadcastDataset(broadcastDatasetID, broadcastDataset);

        final DataflowNodeProperties map2 = map2NodeProperties(broadcastDatasetID, dop);

        final UUID dataset2UID = UUID.randomUUID();

        final DataflowNodeProperties dataset2Properties = dataset2NodeProperties(dataset2UID, dop);

        Topology.AuraTopologyBuilder atb2 = auraClient.createTopologyBuilder();

        atb2.addNode(new Topology.DatasetNode(dataset1Properties)).
                connectTo("Map2", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map2)).
                connectTo("Dataset2", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.DatasetNode(dataset2Properties));

        final Topology.AuraTopology topology2 = atb2.build("testJob3GeneratedDataSetAsSourceForFollowingDataflow-2");

        TestHelper.runTopology(auraClient, topology2);

        auraClient.eraseDataset(dataset1UID);
        auraClient.eraseDataset(dataset2UID);
    }

    /*@Test
    public void testJob4UpdateOperator() {

        int dop =  1; // executionUnits / ?;

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        TypeInformation source1TypeInfo = source1TypeInfo();

        // JOB 1: fill a mutable dataset with state

        final DataflowNodeProperties mutableDatasetSource = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "MutableDatasetSource",
                dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Job4Source.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );

        final UUID mutableDatasetUID = UUID.randomUUID();

        final DataflowNodeProperties mutableDatasetNode = new DataflowNodeProperties(
                mutableDatasetUID,
                DataflowNodeProperties.DataflowNodeType.MUTABLE_DATASET,
                "MutableDataset",
                dop,
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
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                null,
                null
        );

        atb.addNode(new Topology.OperatorNode(mutableDatasetSource)).
                connectTo("MutableDataset", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(mutableDatasetNode));

        final Topology.AuraTopology topology1 = atb.build("JOB1");

        TestHelper.runTopology(auraClient, topology1);

        // ----- JOB 2: update the mutable dataset using an update operator

        final DataflowNodeProperties elementSource = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "MutableDatasetSource",
                dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Job4Source.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );

        Map<String,Object> updateConfig = new HashMap<>();
        updateConfig.put(DatasetUpdatePhysicalOperator.CO_LOCATION_TASK_NAME, mutableDatasetUID);

        final DataflowNodeProperties updateNode = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.DATASET_UPDATE_OPERATOR,
                "Update",
                dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Job4Update.class.getName(),
                null,
                null,
                null,
                null,
                null,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                null,
                updateConfig
        );

        final DataflowNodeProperties sinkNode = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                "Sink",
                dop,
                1,
                null,
                null,
                null,
                null,
                source1TypeInfo,
                Job4Sink.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );

        Topology.AuraTopologyBuilder atb2 = auraClient.createTopologyBuilder();

        atb2.addNode(new Topology.DatasetNode(elementSource)).
                connectTo("Update", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(updateNode)).
                connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.DatasetNode(sinkNode));

        final Topology.AuraTopology topology2 = atb2.build("JOB2");

        TestHelper.runTopology(auraClient, topology2);

        final Collection<Tuple2<String,Integer>> mutableDataset = auraClient.getDataset(mutableDatasetUID);

        for (Tuple2<String,Integer> datasetElement : mutableDataset) {
            System.out.println(datasetElement);
        }

        auraClient.eraseDataset(mutableDatasetUID);
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

    private TypeInformation source1TypeInfo() {
        return new TypeInformation(Tuple2.class,
                new TypeInformation(String.class),
                new TypeInformation(Integer.class));
    }

    // ---------------------------------------------------
    // Dataflow Nodes.
    // ---------------------------------------------------

    private DataflowNodeProperties source1NodeProperties(int dop) {

        TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
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
                Source1.class.getName(),
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
                null,
                null
        );
    }

    private DataflowNodeProperties dataset1NodeProperties(UUID dataset1UID, int dop) {

        TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                dataset1UID,
                DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                "Dataset1",
                dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
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
                null,
                null
        );
    }

    private DataflowNodeProperties map2NodeProperties(UUID broadcastDatasetID, int dop) {

        TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map2",
                dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
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
                Arrays.asList(broadcastDatasetID),
                null
        );
    }

    private DataflowNodeProperties dataset2NodeProperties(UUID dataset2UID, int dop) {

        TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                dataset2UID,
                DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                "Dataset2",
                dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
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
                null,
                null
        );
    }

    // ---------------------------------------------------
    // User-defined Functions.
    // ---------------------------------------------------

    public static final class Source1 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 100;

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

    // ----- TEST JOB 4 UDFs -----

    public static final class Job4Source extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1000;

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ? new Tuple2<>("id=" + count, count) : null;
        }
    }

    public static final class Job4Sink extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {
//            System.out.println(in);
        }
    }

    public static final class Job4Update extends UpdateFunction<Tuple2<String,Integer>,Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> update(Tuple2<String,Integer> state, Tuple2<String,Integer> element) {

            Integer sum = state._2 + element._2;

            return (sum % 2 == 0) ? new Tuple2<>(state._1, sum) : null;
        }
    }
}
