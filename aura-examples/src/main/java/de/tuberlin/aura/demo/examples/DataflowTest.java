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

import java.util.ArrayList;
import java.util.UUID;

public final class DataflowTest {

    // Disallow instantiation.
    private DataflowTest() {}

    // ---------------------------------------------------
    // UDFs.
    // ---------------------------------------------------

    public static final class Source1 extends SourceFunction<Tuple2<Integer, String>> {

        int count = 1000;

        @Override
        public Tuple2<Integer, String> produce() {
            return (--count >= 0 ) ?  new Tuple2<>(count, "String" + count) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<Integer, String>, Tuple2<Integer,String>> {

        @Override
        public Tuple2<Integer,String> map(final Tuple2<Integer, String> in) {
            return in;
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>> {

        @Override
        public void consume(final Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> in) {
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

        final DataflowNodeProperties sourceA = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source1",
                1,
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
                null
        );


        final DataflowNodeProperties sourceB = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source2", 1, 1,
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
                null
        );


        final DataflowNodeProperties map = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map1",
                1,
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
                null
        );


        final TypeInformation join1TypeInfo =
                new TypeInformation(Tuple2.class,
                        source1TypeInfo,
                        source1TypeInfo);

        final DataflowNodeProperties joinA = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.HASH_JOIN_OPERATOR,
                "Join1",
                1,
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


        final TypeInformation join2TypeInfo =
                new TypeInformation(Tuple2.class,
                        join1TypeInfo,
                        source1TypeInfo);

        final DataflowNodeProperties joinB = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.HASH_JOIN_OPERATOR,
                "Join2",
                1,
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


        DataflowNodeProperties sink1 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                "Sink1", 1, 1,
                null,
                null,
                join2TypeInfo,
                null,
                null,
                Sink1.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );

        DataflowNodeProperties sink2 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                "Sink2",
                1,
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
                null
        );

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();
        atb.addNode(new Topology.OperatorNode(sourceA), Source1.class)
           .connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.OperatorNode(map), Map1.class)
           .connectTo("Join1", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.OperatorNode(sourceB), Source1.class)
           .connectTo("Join2", Topology.Edge.TransferType.POINT_TO_POINT)
           .and().connectTo("Join1", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.OperatorNode(joinA), new ArrayList<Class<?>>())
           .connectTo("Join2", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.OperatorNode(joinB), new ArrayList<Class<?>>())
           .connectTo("Sink1", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.OperatorNode(sink1), Sink1.class);

        /*Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();
        atb.addNode(new Topology.OperatorNode(sourceA), Source1.class)
                .connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(map), Map1.class)
                .connectTo("Sink1", Topology.Edge.TransferType.POINT_TO_POINT)
                .and().connectTo("Sink2", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(sink1), Sink1.class)
                .noConnects()
                .addNode(new Topology.OperatorNode(sink2), Sink1.class);*/

        ac.submitTopology(atb.build("JOB1"), null);
        ac.awaitSubmissionResult(1);
        ac.closeSession();
        lcs.shutdown();
    }
}
