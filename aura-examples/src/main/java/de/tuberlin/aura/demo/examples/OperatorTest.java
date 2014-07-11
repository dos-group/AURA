package de.tuberlin.aura.demo.examples;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.operators.OperatorAPI;
import de.tuberlin.aura.core.operators.OperatorProperties;
import de.tuberlin.aura.core.operators.TopologyGenerator;
import de.tuberlin.aura.core.operators.UnaryUDFFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.tuples.Tuple1;
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

    public static final class SourceUDF1 implements UnaryUDFFunction<Void, Tuple1<Integer>> {

        int count = 15;

        @Override
        public Tuple1<Integer> apply(final Void in) {
            return (--count >= 0 ) ?  new Tuple1<>(count) : null;
        }
    }

    public static final class MapUDF1 implements UnaryUDFFunction<Tuple1<Integer>, Tuple2<Integer,String>> {


        @Override
        public Tuple2<Integer,String> apply(final Tuple1<Integer> in) {
            return new Tuple2<>(in._0, toString());
        }
    }

    public static final class SinkUDF1 implements UnaryUDFFunction<Tuple2<Integer,String>, Void> {

        @Override
        public Void apply(final Tuple2<Integer,String> in) {
            System.out.println(in);
            return null;
        }
    }

    public static void main(final String[] args) {

        final OperatorAPI.Operator source1 = new OperatorAPI.Operator(
                new OperatorProperties(
                        OperatorProperties.PhysicalOperatorType.UDF_SOURCE,
                        new int[] {0},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        1,
                        "Source1",
                        SourceUDF1.class,
                        null,
                        null,
                        Tuple1.class
                )
        );

        final OperatorAPI.Operator map1 = new OperatorAPI.Operator(
                new OperatorProperties(
                        OperatorProperties.PhysicalOperatorType.MAP_TUPLE_OPERATOR,
                        new int[] {0},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        1,
                        "Map1",
                        MapUDF1.class,
                        Tuple1.class,
                        null,
                        Tuple2.class
                ),
                source1
        );

        final OperatorAPI.Operator sink1 = new OperatorAPI.Operator(
                new OperatorProperties(
                        OperatorProperties.PhysicalOperatorType.UDF_SINK,
                        1,
                        "Sink1",
                        SinkUDF1.class,
                        Tuple2.class,
                        null,
                        null
                ),
                map1
        );

        //OperatorAPI.PlanPrinter.printPlan(sink1);

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));
        final Topology.AuraTopology topology = new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB1");

        //Topology.TopologyPrinter.printTopology(topology);

        ac.submitTopology(topology, null);
        ac.awaitSubmissionResult();
        ac.closeSession();
        
        lcs.shutdown();

        /*final OperatorAPI.Operator source1 = new OperatorAPI.Operator(
                new OperatorProperties(
                        OperatorProperties.PhysicalOperatorType.UDF_SOURCE,
                        new int[] {0},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        1,
                        "Source1",
                        SourceUDF1.class,
                        null,
                        null,
                        Tuple1.class
                )
        );

        final OperatorAPI.Operator map1 = new OperatorAPI.Operator(
                new OperatorProperties(
                        OperatorProperties.PhysicalOperatorType.MAP_TUPLE_OPERATOR,
                        new int[] {0},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        2,
                        "Map1",
                        MapUDF1.class,
                        Tuple1.class,
                        null,
                        Tuple2.class
                ),
                source1
        );

        final OperatorAPI.Operator source2 = new OperatorAPI.Operator(
                new OperatorProperties(
                        OperatorProperties.PhysicalOperatorType.UDF_SOURCE,
                        new int[] {0},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        1,
                        "Source2",
                        SourceUDF1.class,
                        null,
                        null,
                        Tuple1.class
                )
        );

        final OperatorAPI.Operator union = new OperatorAPI.Operator(
                new OperatorProperties(
                        OperatorProperties.PhysicalOperatorType.HASH_JOIN_OPERATOR,
                        new int[] {0},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        1,
                        "Join1",
                        null,
                        Tuple1.class,
                        null,
                        Tuple2.class
                ),
                map1,
                source2
        );

        final OperatorAPI.Operator sink1 = new OperatorAPI.Operator(
                new OperatorProperties(
                        OperatorProperties.PhysicalOperatorType.UDF_SINK,
                        1,
                        "Sink1",
                        SinkUDF1.class,
                        Tuple2.class,
                        null,
                        null
                ),
                join1
        );*/
    }

    /*public static void resolveTypeParameter(final Class<?> clazz) {

        final Type[] types = clazz.getGenericInterfaces();

        if (types.length > 0) {
            for (final Type type : types) {
                resolve(type);
            }
        }
    }

    public static void resolve(final Type type) {
        if (type instanceof ParameterizedType) {

            final ParameterizedType parameterizedType = (ParameterizedType)type;
            final Type[] typeArguments = parameterizedType.getActualTypeArguments();

            for (final Type typeArgument : typeArguments) {
                if (typeArgument instanceof ParameterizedType) {
                    System.out.println(typeArgument);
                    resolve(typeArgument);
                } else {
                    System.out.println(((Class<?>)typeArgument).getSimpleName());
                }
            }
        }
    }*/
}
