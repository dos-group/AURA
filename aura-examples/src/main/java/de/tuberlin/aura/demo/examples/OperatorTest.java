package de.tuberlin.aura.demo.examples;

import java.util.Random;
import java.util.UUID;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.processing.api.OperatorAPI;
import de.tuberlin.aura.core.processing.api.OperatorProperties;
import de.tuberlin.aura.core.processing.generator.TopologyGenerator;
import de.tuberlin.aura.core.processing.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.processing.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;

/**
 *
 */
public class OperatorTest {

    // Disallow instantiation.
    private OperatorTest() {}

    // ------------------------------------------------------------------------------------------------
    // Testing.
    // ------------------------------------------------------------------------------------------------

    public static final class Source1 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1000;

        Random rand = new Random(12312);

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE1", rand.nextInt(10000)) : null;
        }
    }

    public static final class Source2 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1000;

        Random rand = new Random(13454);

        @Override
        public Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE2", rand.nextInt(10000)) : null;
        }
    }

    /*public static final class Map1 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            return new Tuple2<>(in._0, in._1);
        }
    }*/

    public static final class Sink1 extends SinkFunction<Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>>> {

        @Override
        public void consume(final Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>> in) {
            System.out.println(in);
        }
    }


    public static void main(final String[] args) {

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final OperatorAPI.Operator source1 =
                new OperatorAPI.Operator(
                        new OperatorProperties(
                                UUID.randomUUID(),
                                OperatorProperties.PhysicalOperatorType.UDF_SOURCE,
                                1,
                                new int[][] { source1TypeInfo.buildSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                2,
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

        final OperatorAPI.Operator source2 =
                new OperatorAPI.Operator(
                        new OperatorProperties(
                                UUID.randomUUID(),
                                OperatorProperties.PhysicalOperatorType.UDF_SOURCE,
                                1,
                                new int[][] { source2TypeInfo.buildSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                2,
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

        final OperatorAPI.Operator join1 =
                new OperatorAPI.Operator(
                        new OperatorProperties(
                                UUID.randomUUID(),
                                OperatorProperties.PhysicalOperatorType.HASH_JOIN_OPERATOR,
                                1,
                                new int[][] { join1TypeInfo.buildSelectorChain("_0._1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                2,
                                "Join1",
                                source1TypeInfo,
                                source2TypeInfo,
                                join1TypeInfo,
                                null,
                                new int[][] { source1TypeInfo.buildSelectorChain("_1") },
                                new int[][] { source2TypeInfo.buildSelectorChain("_1") },
                                null,
                                null
                        ),
                        source1,
                        source2
                );


        final OperatorAPI.Operator sort1 =
                new OperatorAPI.Operator(
                        new OperatorProperties(
                                UUID.randomUUID(),
                                OperatorProperties.PhysicalOperatorType.SORT_OPERATOR,
                                1,
                                new int[][] { join1TypeInfo.buildSelectorChain("_0._1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                2,
                                "Sort1",
                                join1TypeInfo,
                                null,
                                join1TypeInfo,
                                null,
                                null,
                                null,
                                new int[][] { join1TypeInfo.buildSelectorChain("_1._1") },
                                OperatorProperties.SortOrder.DESCENDING
                        ),
                        join1
                );


        final OperatorAPI.Operator sink1 =
                new OperatorAPI.Operator(
                        new OperatorProperties(
                                UUID.randomUUID(),
                                OperatorProperties.PhysicalOperatorType.UDF_SINK,
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
                        sort1
                );


        // OperatorAPI.PlanPrinter.printPlan(sink1);

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));
        final Topology.AuraTopology topology = new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB1");

        //Topology.TopologyPrinter.printTopology(topology);

        ac.submitTopology(topology, null);
        ac.awaitSubmissionResult(1);
        ac.closeSession();

        lcs.shutdown();

        /*final OperatorAPI.Operator union1 =
        new OperatorAPI.Operator(
                new OperatorProperties(
                        UUID.randomUUID(),
                        OperatorProperties.PhysicalOperatorType.UNION_OPERATOR,
                        1,
                        new int[] {0},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        1,
                        "Union1",
                        Tuple3.class,
                        Tuple3.class,
                        Tuple3.class,
                        null,
                        null,
                        null,
                        null,
                        null
                ),
                source1,
                source2
        );*/

        /*final OperatorAPI.Operator map1 =
        new OperatorAPI.Operator(
                new OperatorProperties(
                        UUID.randomUUID(),
                        OperatorProperties.PhysicalOperatorType.MAP_TUPLE_OPERATOR,
                        1,
                        new int[] {0},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        1,
                        "Map1",
                        Tuple3.class,
                        null,
                        Tuple3.class,
                        Map1.class,
                        null,
                        null,
                        null,
                        null
                ),
                source2
        );*/
    }
}
