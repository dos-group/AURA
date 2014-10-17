package de.tuberlin.aura.tests.integration;

import java.util.*;

import de.tuberlin.aura.core.record.tuples.Tuple1;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.*;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.tests.util.TestHelper;

public final class ParallelDataflowTest {

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
    public void testParallelDataflows() {

        // we can only use ~ 8 execution units on Travis
        int parallelSubmissions = (executionUnits < 9) ? 2 : 3;

        int dop = (executionUnits / 3) / parallelSubmissions;

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple1.class, new TypeInformation(Integer.class));

        final TypeInformation map1TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(Integer.class),
                        new TypeInformation(String.class));

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
                null, null, null, null, null,
                null, null, null, null
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
                map1TypeInfo,
                Map1.class.getName(),
                null, null, null, null, null,
                null, null, null, null
        );

        final DataflowNodeProperties sink1 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                "Sink1",
                1,
                1,
                null,
                null,
                map1TypeInfo,
                null,
                null,
                Sink1.class.getName(),
                null, null, null, null, null,
                null, null, null, null
        );

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source1)).
                connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map1)).
                connectTo("Sink1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(sink1));

        // submitting concurrently from different threads sometimes yields
        // an error locally: DataConsumer.getInputGateIndexFromTaskID --> NullPointerException

        List<Topology.AuraTopology> topologies = new ArrayList<>();

        for (int i = 1; i <= parallelSubmissions; i++) {
            topologies.add(atb.build("JOB" + i));
        }

        TestHelper.runTopologiesInParallel(auraClient, topologies);
    }

    @AfterClass
    public static void tearDown() {

        if (!IntegrationTestSuite.isRunning) {
            IntegrationTestSuite.tearDownTestEnvironment();
        }

        auraClient.closeSession();
    }

    // ---------------------------------------------------
    // User-defined Functions.
    // ---------------------------------------------------

    public static final class Source1 extends SourceFunction<Tuple1<Integer>> {

        int count = 1500000;

        @Override
        public Tuple1<Integer> produce() {
            return (--count >= 0 ) ?  new Tuple1<>(count) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple1<Integer>, Tuple2<Integer,String>> {

        @Override
        public Tuple2<Integer,String> map(final Tuple1<Integer> in) {
            return new Tuple2<>(in._1, toString());
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple2<Integer,String>> {

        @Override
        public void consume(final Tuple2<Integer,String> in) {
            //System.out.println(in);
        }
    }

}
