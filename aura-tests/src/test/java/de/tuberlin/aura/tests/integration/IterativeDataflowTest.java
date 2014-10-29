package de.tuberlin.aura.tests.integration;


import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.datasets.AbstractDataset;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class IterativeDataflowTest {

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
    public void testSimpleIterativeDataflow() {

        int dop = executionUnits / 4;

        final DataflowNodeProperties source1 = new DataflowNodeProperties(
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

        final UUID dataset1UID = UUID.randomUUID();

        final DataflowNodeProperties dataset1 = new DataflowNodeProperties(
                dataset1UID,
                DataflowNodeProperties.DataflowNodeType.DATASET_REFERENCE,
                "DatasetHead",
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
                null,
                null,
                null
        );

        final DataflowNodeProperties map1 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map1",
                dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
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

        final UUID dataset2UID = UUID.randomUUID();

        final Map<String,Object> tailDatasetConfig = new HashMap<>();
        tailDatasetConfig.put(AbstractPhysicalOperator.CO_LOCATION_TASK_NAME, "DatasetHead");

        final DataflowNodeProperties dataset2 = new DataflowNodeProperties(
                dataset2UID,
                DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                "DatasetTail",
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
                null,
                null,
                tailDatasetConfig
        );

        DataflowNodeProperties sink1 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                "Sink1",
                dop,
                1,
                null,
                null,
                source1TypeInfo,
                null,
                null,
                Sink1.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );

        // ---------------------------------------------------

        final Topology.AuraTopologyBuilder atb1 = auraClient.createTopologyBuilder();

        atb1.addNode(new Topology.OperatorNode(source1))
                .connectTo("DatasetHead", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.DatasetNode(dataset1));

        final Topology.AuraTopology topology1 = atb1.build("ITERATION-JOB1");
        auraClient.submitTopology(topology1, null);
        auraClient.awaitSubmissionResult(1);

        // ---------------------------------------------------

        final Topology.AuraTopologyBuilder atb2 = auraClient.createTopologyBuilder();
        atb2.addNode(new Topology.DatasetNode(dataset1, AbstractDataset.DatasetType.DATASET_ITERATION_HEAD_STATE))
                .connectTo("Map1", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.OperatorNode(map1))
                .connectTo("DatasetTail", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.DatasetNode(dataset2, AbstractDataset.DatasetType.DATASET_ITERATION_TAIL_STATE));

        final Topology.AuraTopology topology2 = atb2.build("JOB2", true);
        auraClient.submitTopology(topology2, null);

        final int ITERATION_COUNT = 100;

        for (int i = 0; i < ITERATION_COUNT; ++i) {

            auraClient.waitForIterationEnd(topology2.topologyID);
            auraClient.assignDataset(dataset1UID, dataset2UID);
            auraClient.reExecute(topology2.topologyID, i < ITERATION_COUNT - 1);

            System.out.println("iteration count = " + i);
        }

        auraClient.awaitSubmissionResult(1);
        auraClient.assignDataset(dataset1UID, dataset2UID);

        // ---------------------------------------------------

        final Topology.AuraTopologyBuilder atb3 = auraClient.createTopologyBuilder();
        atb3.addNode(new Topology.DatasetNode(dataset1))
                .connectTo("Sink1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(sink1));

        final Topology.AuraTopology topology3 = atb3.build("JOB3");
        auraClient.submitTopology(topology3, null);
        auraClient.awaitSubmissionResult(1);
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

    private static final TypeInformation source1TypeInfo =
            new TypeInformation(Tuple2.class,
                    new TypeInformation(Integer.class),
                    new TypeInformation(String.class));

    // ---------------------------------------------------
    // User-defined Functions.
    // ---------------------------------------------------

    public static final int COUNT = 10000;

    public static final class Source1 extends SourceFunction<Tuple2<Integer, String>> {

        int count = COUNT;

        @Override
        public Tuple2<Integer, String> produce() {
            Tuple2<Integer, String> res = (--count >= 0 ) ?  new Tuple2<>(count, "String" + count) : null;
            if (count < 0) count = COUNT;
            return res;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<Integer, String>, Tuple2<Integer,String>> {

        @Override
        public Tuple2<Integer,String> map(final Tuple2<Integer, String> in) {
            return new Tuple2<>(in._1 + 1, in._2);
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {
            /*System.out.println(in);*/
        }
    }
}
