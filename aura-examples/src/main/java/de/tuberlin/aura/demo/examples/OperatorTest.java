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

import java.util.Random;
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

        int count = 950000;

        Random rand = new Random(12312);

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE1", rand.nextInt(10000)) : null;
        }
    }

    public static final class Source2 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 950000;

        Random rand = new Random(13454);

        @Override
        public Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE2", rand.nextInt(10000)) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<Tuple2<String,Integer>,Tuple2<String,Integer>>, Tuple2<Tuple2<String,Integer>,Tuple2<String,Integer>>> {

        @Override
        public Tuple2<Tuple2<String,Integer>,Tuple2<String,Integer>> map(final Tuple2<Tuple2<String,Integer>,Tuple2<String,Integer>> in) {
            return new Tuple2<>(new Tuple2<>("HELLO", in._0._1), in._1);
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>>> {

        @Override
        public void consume(final Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>> in) {
            System.out.println(in);
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

        final TypeInformation source2TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor source2 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                1,
                                new int[][] { source2TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Source2",
                                null,
                                null,
                                source2TypeInfo,
                                Source2.class,
                                null,
                                null,
                                null,
                                null
                        )
                );

        final TypeInformation join1TypeInfo =
                new TypeInformation(Tuple2.class,
                        source1TypeInfo,
                        source2TypeInfo);

        final DataflowAPI.DataflowNodeDescriptor join1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.HASH_JOIN_OPERATOR,
                                1,
                                new int[][] { join1TypeInfo.buildFieldSelectorChain("_0._1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Join1",
                                source1TypeInfo,
                                source2TypeInfo,
                                join1TypeInfo,
                                null,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                new int[][] { source2TypeInfo.buildFieldSelectorChain("_1") },
                                null,
                                null
                        ),
                        source1,
                        source2
                );

        final DataflowAPI.DataflowNodeDescriptor sort1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                                1,
                                new int[][] { join1TypeInfo.buildFieldSelectorChain("_0._1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Sort1",
                                join1TypeInfo,
                                null,
                                join1TypeInfo,
                                null,
                                null,
                                null,
                                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._1") },
                                DataflowNodeProperties.SortOrder.DESCENDING
                        ),
                        join1
                );

        final DataflowAPI.DataflowNodeDescriptor map1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                                1,
                                new int[][] {join1TypeInfo.buildFieldSelectorChain("_0._1")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Map1",
                                join1TypeInfo,
                                null,
                                join1TypeInfo,
                                Map1.class,
                                null,
                                null,
                                null,
                                null
                        ),
                        sort1
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
                                join1TypeInfo,
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
