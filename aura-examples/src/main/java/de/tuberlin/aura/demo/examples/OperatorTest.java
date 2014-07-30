package de.tuberlin.aura.demo.examples;

import java.util.UUID;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.processing.api.OperatorAPI;
import de.tuberlin.aura.core.processing.api.OperatorProperties;
import de.tuberlin.aura.core.processing.generator.TopologyGenerator;
import de.tuberlin.aura.core.processing.udfs.functions.MapFunction;
import de.tuberlin.aura.core.processing.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.processing.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.record.tuples.Tuple3;
import de.tuberlin.aura.core.record.tuples.Tuple4;
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

    public static final class Source1 extends SourceFunction<Tuple3<String,Integer,Integer>> {

        int count = 1000;

        @Override
        public  Tuple3<String,Integer,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple3<>("KEY", count, 1) : null;
        }
    }

    public static final class Source2 extends SourceFunction<Tuple3<String,Integer,Integer>> {

        int count = 1000;

        @Override
        public Tuple3<String,Integer,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple3<>("KEY", count, 1) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple3<String,Integer,Integer>, Tuple3<String,Integer,Integer>> {

        @Override
        public Tuple3<String,Integer,Integer> map(final Tuple3<String,Integer,Integer> in) {
            return new Tuple3<>(in._0, in._1, in._2);
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple2<Tuple3<String,Integer,Integer>, Tuple3<String,Integer,Integer>>> {

        @Override
        public void consume(Tuple2<Tuple3<String,Integer,Integer>, Tuple3<String,Integer,Integer>> in) {

            getEnvironment().getLogger().info(in.toString());
        }
    }

    public static void main(final String[] args) {

        final TypeInformation source1OutputTypeInfo =
                new TypeInformation(Tuple3.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class),
                        new TypeInformation(Integer.class));

        final OperatorAPI.Operator source1 =
                new OperatorAPI.Operator(
                        new OperatorProperties(
                                UUID.randomUUID(),
                                OperatorProperties.PhysicalOperatorType.UDF_SOURCE,
                                1,
                                new int[][] { {0} },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Source1",
                                null,
                                null,
                                source1OutputTypeInfo,
                                Source1.class,
                                null,
                                null,
                                null,
                                null
                        )
                );

        final TypeInformation source2OutputTypeInfo =
                new TypeInformation(Tuple3.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class),
                        new TypeInformation(Integer.class));

        final OperatorAPI.Operator source2 =
                new OperatorAPI.Operator(
                        new OperatorProperties(
                                UUID.randomUUID(),
                                OperatorProperties.PhysicalOperatorType.UDF_SOURCE,
                                1,
                                new int[][] { {0} },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Source2",
                                null,
                                null,
                                source2OutputTypeInfo,
                                Source2.class,
                                null,
                                null,
                                null,
                                null
                        )
                );

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

        /*final OperatorAPI.Operator sort1 =
                new OperatorAPI.Operator(
                        new OperatorProperties(
                                UUID.randomUUID(),
                                OperatorProperties.PhysicalOperatorType.SORT_OPERATOR,
                                1,
                                new int[] {0},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Sort1",
                                Tuple3.class,
                                null,
                                Tuple3.class,
                                null,
                                null,
                                null,
                                new int[] {0},
                                OperatorProperties.SortOrder.DESCENDING
                        ),
                        map1
                );*/


        final TypeInformation join1OutputTypeInfo =
                new TypeInformation(Tuple2.class,
                        source1OutputTypeInfo,
                        source2OutputTypeInfo);

        final OperatorAPI.Operator join1 =
                new OperatorAPI.Operator(
                        new OperatorProperties(
                                UUID.randomUUID(),
                                OperatorProperties.PhysicalOperatorType.HASH_JOIN_OPERATOR,
                                1,
                                new int[][] { {0} },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Join1",
                                source1OutputTypeInfo,
                                source2OutputTypeInfo,
                                join1OutputTypeInfo,
                                null,
                                new int[][] { { 0 }, { 1 } },
                                new int[][] { { 0 }, { 1 } },
                                null,
                                null
                        ),
                        source1,
                        source2
                );

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
                                join1OutputTypeInfo,
                                null,
                                null,
                                Sink1.class,
                                null,
                                null,
                                null,
                                null
                        ),
                        join1
                );

        // OperatorAPI.PlanPrinter.printPlan(sink1);

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));
        final Topology.AuraTopology topology = new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB1");

        // Topology.TopologyPrinter.printTopology(topology);

        ac.submitTopology(topology, null);
        ac.awaitSubmissionResult(1);
        ac.closeSession();

        lcs.shutdown();
    }
}
