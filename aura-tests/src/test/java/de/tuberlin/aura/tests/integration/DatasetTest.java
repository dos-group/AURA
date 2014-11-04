package de.tuberlin.aura.tests.integration;


import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class DatasetTest {

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
    public void testSimpleSerializedDataset() {

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
                DataflowNodeProperties.DataflowNodeType.SERIALIZED_IMMUTABLE_DATASET,
                "Dataset1",
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

        final Topology.AuraTopologyBuilder atb1 = auraClient.createTopologyBuilder();
        atb1.addNode(new Topology.OperatorNode(source1))
                .connectTo("Dataset1", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.DatasetNode(dataset1));

        final Topology.AuraTopology topology1 = atb1.build("JOB-ProduceSerializedDataset");
        auraClient.submitTopology(topology1, null);
        auraClient.awaitSubmissionResult(1);

        final Topology.AuraTopologyBuilder atb2 = auraClient.createTopologyBuilder();
        atb2.addNode(new Topology.DatasetNode(dataset1))
                .connectTo("Sink1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(sink1));

        final Topology.AuraTopology topology2 = atb2.build("JOB-ConsumeSerializedDataset");
        auraClient.submitTopology(topology2, null);
        auraClient.awaitSubmissionResult(1);

        auraClient.eraseDataset(dataset1UID);
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
    // UDFs.
    // ---------------------------------------------------

    public static final int COUNT = 10000;

    public static final class Source1 extends SourceFunction<Tuple2<Integer, String>> {

        int count = COUNT;

        @Override
        public Tuple2<Integer, String> produce() {
            return (--count >= 0 ) ?  new Tuple2<>(count, "String" + count) : null;
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {
                //System.out.println(in);
        }
    }
}

