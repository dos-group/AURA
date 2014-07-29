package de.tuberlin.aura.demo.examples;

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
import de.tuberlin.aura.core.record.tuples.Tuple1;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;

import java.util.UUID;

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

        final OperatorAPI.Operator source1 = new OperatorAPI.Operator(
                new OperatorProperties(
                        UUID.randomUUID(),
                        OperatorProperties.PhysicalOperatorType.UDF_SOURCE,
                        1,
                        new int[] {0},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        1,
                        "Source1",
                        null,
                        null,
                        Tuple1.class,
                        Source1.class,
                        null,
                        null,
                        null,
                        null
                )
        );

        final OperatorAPI.Operator map1 = new OperatorAPI.Operator(
                new OperatorProperties(
                        UUID.randomUUID(),
                        OperatorProperties.PhysicalOperatorType.MAP_TUPLE_OPERATOR,
                        1,
                        new int[] {0},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        1,
                        "Map1",
                        Tuple1.class,
                        null,
                        Tuple2.class,
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
                        Tuple2.class,
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