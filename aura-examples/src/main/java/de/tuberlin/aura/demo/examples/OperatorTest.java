package de.tuberlin.aura.demo.examples;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.generator.TopologyGenerator;
import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowAPI;
import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;

import java.util.UUID;

/**
 *
 */
public final class OperatorTest {

    // Disallow instantiation.
    private OperatorTest() {}

    // ---------------------------------------------------
    // User Defined Functions.
    // ---------------------------------------------------

    public static final class Source1 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 350000;

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE1", count) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            return new Tuple2<>("HELLO", in._1);
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {

            //if (in._1 % 1 == 0)
            //    System.out.println(in);
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor source1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Source1",
                                null,
                                null,
                                source1TypeInfo,
                                Source1.class,
                                null,
                                null,
                                null,
                                null
                        )
                );

        final DataflowAPI.DataflowNodeDescriptor map1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                                1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                2,
                                "Map1",
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                Map1.class,
                                null,
                                null,
                                null,
                                null
                        ),
                        source1
                );

        final DataflowAPI.DataflowNodeDescriptor sink1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                                1,
                                null,
                                null,
                                1,
                                "Sink1",
                                source1TypeInfo,
                                null,
                                null,
                                Sink1.class,
                                null,
                                null,
                                null,
                                null
                        ),
                        map1
                );

        final Topology.AuraTopology topology1 = new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB1");
        ac.submitTopology(topology1, null);
        ac.awaitSubmissionResult(1);
        ac.closeSession();
        lcs.shutdown();
    }
}
