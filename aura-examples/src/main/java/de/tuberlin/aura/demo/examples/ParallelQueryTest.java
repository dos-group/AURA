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
import de.tuberlin.aura.core.record.tuples.Tuple1;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.record.tuples.Tuple3;
import de.tuberlin.aura.core.topology.Topology;

/**
 *
 */
public class ParallelQueryTest {

    // Disallow instantiation.
    private ParallelQueryTest() {}

    // ------------------------------------------------------------------------------------------------
    // Testing.
    // ------------------------------------------------------------------------------------------------

    public static final class Source1 extends SourceFunction<Tuple1<Integer>> {

        int count = 15;

        @Override
        public Tuple1<Integer> produce() {
            return (--count >= 0 ) ?  new Tuple1<>(count) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple1<Integer>, Tuple2<Integer,String>> {

        @Override
        public Tuple2<Integer,String> map(final Tuple1<Integer> in) {
            return new Tuple2<>(in._0, toString());
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple2<Integer,String>> {

        @Override
        public void consume(final Tuple2<Integer,String> in) {
            System.out.println(in);
        }
    }

    public static void main(final String[] args) {

        final TypeInformation source1OutputTypeInfo =
                new TypeInformation(Tuple1.class,
                        new TypeInformation(Integer.class));

        final OperatorAPI.Operator source1 = new OperatorAPI.Operator(
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

        final TypeInformation map1OutputTypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(Integer.class),
                        new TypeInformation(String.class));

        final OperatorAPI.Operator map1 = new OperatorAPI.Operator(
                new OperatorProperties(
                        UUID.randomUUID(),
                        OperatorProperties.PhysicalOperatorType.MAP_TUPLE_OPERATOR,
                        1,
                        new int[][] { {0} },
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        1,
                        "Map1",
                        source1OutputTypeInfo,
                        null,
                        map1OutputTypeInfo,
                        Map1.class,
                        null,
                        null,
                        null,
                        null
                ),
                source1
        );

        final OperatorAPI.Operator sink1 = new OperatorAPI.Operator(
                new OperatorProperties(
                        UUID.randomUUID(),
                        OperatorProperties.PhysicalOperatorType.UDF_SINK,
                        1,
                        null,
                        null,
                        1,
                        "Sink1",
                        map1OutputTypeInfo,
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

        // Local
        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        final Topology.AuraTopology topology1 = new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB1");
        ac.submitTopology(topology1, null);

        final Topology.AuraTopology topology2 = new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB2");
        ac.submitTopology(topology2, null);

        final Topology.AuraTopology topology3 = new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB3");
        ac.submitTopology(topology3, null);

        //ac.awaitSubmissionResult();
        //ac.closeSession();
        //lcs.shutdown();
    }
}
