package de.tuberlin.aura.demo.examples;


import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.*;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;

import java.util.*;

public class CompactificationTest {

    public static final class Source1 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1000;

        Random rand = new Random(13454);

        @Override
        public Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE1", count) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            return new Tuple2<>(in._1 + "A", in._2);
        }
    }

    public static final class Map2 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            return new Tuple2<>(in._1 + "B", in._2);
        }
    }

    public static final class Map3 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            return new Tuple2<>(in._1 + "C", in._2);
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
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));


        // ---------------------------------------------------

        DataflowNodeProperties source1 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source1",
                1,
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
                null,
                null
        );

        DataflowNodeProperties map1 = new DataflowNodeProperties(
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
                null,
                null
        );

        DataflowNodeProperties map2 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map2",
                1,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
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
                null,
                null
        );

        DataflowNodeProperties map3 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map3",
                1,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                Map3.class.getName(),
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
                null,
                null
        );

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));


        Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();
        atb.addNode(new Topology.OperatorNode(source1))
                .connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(Arrays.asList(map1, map2, map3)))
                .connectTo("Sink1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(sink1));


        ac.submitTopology(atb.build("JOB1"), null);

        ac.awaitSubmissionResult(1);
        ac.closeSession();
        lcs.shutdown();
    }
}
