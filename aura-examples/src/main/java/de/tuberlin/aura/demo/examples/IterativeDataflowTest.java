package de.tuberlin.aura.demo.examples;


import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;

import java.util.UUID;

public final class IterativeDataflowTest {

    // Disallow Instantiation.
    public IterativeDataflowTest() {}

    // ---------------------------------------------------
    // UDFs.
    // ---------------------------------------------------

    public static final int COUNT = 5;

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
            return in;
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {
            System.out.println(in);
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(Integer.class),
                        new TypeInformation(String.class));

        final DataflowNodeProperties source1 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source1",
                3,
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

        final DataflowNodeProperties map1 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map1",
                3,
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

        DataflowNodeProperties sink1 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                "Sink1",
                3,
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

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();
        atb.addNode(new Topology.OperatorNode(source1), Source1.class)
                .connectTo("Map1", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.OperatorNode(map1), Map1.class)
                .connectTo("Sink1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(sink1), Sink1.class);

        final Topology.AuraTopology topology = atb.build("ITERATIVE_JOB1", true);

        ac.submitTopology(topology, null);

        for (int i = 0; i < 5; ++i) {
            ac.waitForIterationEnd(topology.topologyID);
            ac.reExecute(topology.topologyID, i < 5 - 1);
        }

        ac.awaitSubmissionResult(1);

        ac.closeSession();
        lcs.shutdown();
    }
}

