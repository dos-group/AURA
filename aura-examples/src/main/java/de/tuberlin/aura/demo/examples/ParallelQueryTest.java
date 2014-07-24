package de.tuberlin.aura.demo.examples;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.operators.IUnaryUDFFunction;
import de.tuberlin.aura.core.operators.OperatorAPI;
import de.tuberlin.aura.core.operators.OperatorProperties;
import de.tuberlin.aura.core.operators.TopologyGenerator;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.tuples.Tuple1;
import de.tuberlin.aura.core.record.tuples.Tuple2;
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

    public static final class SourceUDF1 implements IUnaryUDFFunction<Void, Tuple1<Integer>> {

        int count = 15;

        @Override
        public Tuple1<Integer> apply(final Void in) {
            return (--count >= 0 ) ?  new Tuple1<>(count) : null;
        }
    }

    public static final class MapUDF1 implements IUnaryUDFFunction<Tuple1<Integer>, Tuple2<Integer,String>> {


        @Override
        public Tuple2<Integer,String> apply(final Tuple1<Integer> in) {
            return new Tuple2<>(in._0, toString());
        }
    }

    public static final class SinkUDF1 implements IUnaryUDFFunction<Tuple2<Integer,String>, Void> {

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

        // Local
        final String zookeeperAddress = "localhost:2181";
        final LocalClusterSimulator lcs = new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, 4);
        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

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
