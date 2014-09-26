package de.tuberlin.aura.demo.examples;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.generator.TopologyGenerator;
import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowAPI;
import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.*;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;

/**
 *
 */
public final class OperatorTest {

    // Disallow instantiation.
    private OperatorTest() {}

    // ---------------------------------------------------
    // User-defined Functions.
    // ---------------------------------------------------

    public static final class Source1 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1200000;

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE1", count) : null;
        }
    }

    public static final class Source2 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 100000;

        Random rand = new Random(13454);

        @Override
        public Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("RIGHT_SOURCE", rand.nextInt(10000)) : null;
        }
    }

    public static final class Source3 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1000;

        Random rand = new Random(54321);

        @Override
        public Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("RIGHT_SOURCE", rand.nextInt(10000)) : null;
        }
    }

    public static final class Source4 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 100000;

        Random rand = new Random(13454);

        @Override
        public Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("RIGHT_SOURCE", rand.nextInt(100)) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            return new Tuple2<>("HELLO", in._1);
        }
    }

    public static final class FlatMap1 extends FlatMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public void flatMap(Tuple2<String,Integer> in, Collection<Tuple2<String,Integer>> c) {
            if ((in._1 % 10) == 0) {
                c.add(new Tuple2<>("HEL", in._1));
                c.add(new Tuple2<>("LO", in._1 + 1));
            }
        }

    }

    public static final class GroupMap1 extends GroupMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public void map(Iterator<Tuple2<String,Integer>> in, Collection<Tuple2<String,Integer>> output) {

            Integer count = 0;

            while (in.hasNext()) {
                Tuple2<String,Integer> t = in.next();
                output.add(new Tuple2<>(t._0, t._1 + count++));
            }
        }

    }

    // TODO: have Fold1 return an integer instead.. ??
    public static final class Fold1 extends FoldFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> initialValue() {
            return new Tuple2<>("RESULT", 0);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> in) {
            return new Tuple2<>(in._0, 1);
        }

        @Override
        public Tuple2<String,Integer> add(Tuple2<String,Integer> currentValue, Tuple2<String, Integer> in) {
            return new Tuple2<>("RESULT", currentValue._1 + in._1);
        }
    }

    public static final class Filter1 extends FilterFunction<Tuple2<String,Integer>> {

        @Override
        public boolean filter(final Tuple2<String,Integer> in) {
            return in._1 % 2 == 0;
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple2<String,Integer>> {

        @Override
        public void consume(final Tuple2<String,Integer> in) {
//            System.out.println(in);
        }
    }

    public static final class JoinSink1 extends SinkFunction<Tuple2<Tuple2<String,Integer>,Tuple2<String,Integer>>> {

        @Override
        public void consume(final Tuple2<Tuple2<String,Integer>,Tuple2<String,Integer>> in) {
//            System.out.println(in);
        }
    }

    public static Topology.AuraTopology testJob1(AuraClient ac) {

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
                                2,
                                "Source1",
                                null,
                                null,
                                source1TypeInfo,
                                Source1.class,
                                null,
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
                                1,
                                "Map1",
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                Map1.class,
                                null,
                                null,
                                null,
                                null,
                                null
                        ),
                        source1
                );

        final DataflowAPI.DataflowNodeDescriptor flatMap1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.FLAT_MAP_TUPLE_OPERATOR,
                                1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "FlatMap1",
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                FlatMap1.class,
                                null,
                                null,
                                null,
                                null,
                                null
                        ),
                        map1
                );

        final DataflowAPI.DataflowNodeDescriptor filter1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.FILTER_OPERATOR,
                                1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Filter1",
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                Filter1.class,
                                null,
                                null,
                                null,
                                null,
                                null
                        ),
                        flatMap1
                );

        final DataflowAPI.DataflowNodeDescriptor fold1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                                1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,  // TODO: when DOP > 2, how to get the sink1 to print both partial results?
                                "Fold1",
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                Fold1.class,
                                null,
                                null,
                                null,
                                null,
                                null
                        ),
                        filter1
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
                                null,
                                null
                        ),
                        fold1
                );

        return new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB1");
    }

    public static Topology.AuraTopology testJob2(AuraClient ac) {

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
                                null,
                                null
                        )
                );

        final DataflowAPI.DataflowNodeDescriptor source2 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Source2",
                                null,
                                null,
                                source1TypeInfo,
                                Source2.class,
                                null,
                                null,
                                null,
                                null,
                                null
                        )
                );

        final DataflowAPI.DataflowNodeDescriptor source3 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Source3",
                                null,
                                null,
                                source1TypeInfo,
                                Source3.class,
                                null,
                                null,
                                null,
                                null,
                                null
                        )
                );

        final DataflowAPI.DataflowNodeDescriptor difference1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.DIFFERENCE_OPERATOR,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Difference1",
                                source1TypeInfo,
                                source1TypeInfo,
                                source1TypeInfo,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                        ),
                        source2,
                        source3
                );

        final TypeInformation join1TypeInfo =
                new TypeInformation(Tuple2.class,
                        source1TypeInfo,
                        source1TypeInfo);

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
                                source1TypeInfo,
                                join1TypeInfo,
                                null,
                                null,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                null,
                                null
                        ),
                        source1,
                        difference1
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
                                null,
                                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._1") },
                                DataflowNodeProperties.SortOrder.DESCENDING
                        ),
                        join1
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
                                JoinSink1.class,
                                null,
                                null,
                                null,
                                null,
                                null
                        ),
                        sort1
                );

        return new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB2");
    }

    public static Topology.AuraTopology testJob3(AuraClient ac) {

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor source4 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                2,
                                "Source4",
                                null,
                                null,
                                source1TypeInfo,
                                Source4.class,
                                null,
                                null,
                                null,
                                null,
                                null
                        )
                );

        // the optimizer will insert Sorts before GroupBys (using the same keys for sorting as for grouping)
        final DataflowAPI.DataflowNodeDescriptor sort1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                2,
                                "Sort1",
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                null,
                                null,
                                null,
                                null,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                DataflowNodeProperties.SortOrder.ASCENDING
                        ),
                        source4
                );

        final TypeInformation groupBy1TypeInfo =
                new TypeInformation(Tuple2.class, true,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor groupBy1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                4,
                                "GroupBy1",
                                source1TypeInfo,
                                null,
                                groupBy1TypeInfo,
                                null,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                null,
                                null,
                                null,
                                null
                        ),
                        sort1
                );

        final DataflowAPI.DataflowNodeDescriptor fold1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                                1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                4,  // TODO: when DOP > 2, how to get the sink1 to print all partial results?
                                "Fold1",
                                groupBy1TypeInfo,
                                null,
                                source1TypeInfo,
                                Fold1.class,
                                null,
                                null,
                                null,
                                null,
                                null
                        ),
                        groupBy1
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
                                null,
                                null
                        ),
                        fold1
                );

        return new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB3");
    }

    public static Topology.AuraTopology testJob4(AuraClient ac) {

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor source4 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Source4",
                                null,
                                null,
                                source1TypeInfo,
                                Source4.class,
                                null,
                                null,
                                null,
                                null,
                                null
                        )
                );

        // the optimizer will insert Sorts before GroupBys (using the same keys for sorting as for grouping)
        final DataflowAPI.DataflowNodeDescriptor sort1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "Sort1",
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                null,
                                null,
                                null,
                                null,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                DataflowNodeProperties.SortOrder.ASCENDING
                        ),
                        source4
                );

        final TypeInformation groupBy1TypeInfo =
                new TypeInformation(Tuple2.class, true,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowAPI.DataflowNodeDescriptor groupBy1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                                1,
                                null,
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "GroupBy1",
                                source1TypeInfo,
                                null,
                                groupBy1TypeInfo,
                                null,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                null,
                                null,
                                null,
                                null
                        ),
                        sort1
                );

        final DataflowAPI.DataflowNodeDescriptor mapGroup1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.MAP_GROUP_OPERATOR,
                                1,
                                null,
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,
                                "MapGroup1",
                                groupBy1TypeInfo,
                                null,
                                groupBy1TypeInfo,
                                GroupMap1.class,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                null,
                                null,
                                null,
                                null
                        ),
                        groupBy1
                );

        final DataflowAPI.DataflowNodeDescriptor fold1 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                                1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                1,  // TODO: when DOP > 2, how to get the sink1 to print both partial results?
                                "Fold1",
                                groupBy1TypeInfo,
                                null,
                                source1TypeInfo,
                                Fold1.class,
                                null,
                                null,
                                null,
                                null,
                                null
                        ),
                        mapGroup1
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
                                null,
                                null
                        ),
                        fold1
                );

        return new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB4");
    }


    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        final Topology.AuraTopology topology1 = testJob1(ac);
        ac.submitTopology(topology1, null);
        ac.awaitSubmissionResult(1);

        final Topology.AuraTopology topology2 = testJob2(ac);
        ac.submitTopology(topology2, null);
        ac.awaitSubmissionResult(1);

        final Topology.AuraTopology topology3 = testJob3(ac);
        ac.submitTopology(topology3, null);
        ac.awaitSubmissionResult(1);

        final Topology.AuraTopology topology4 = testJob4(ac);
        ac.submitTopology(topology4, null);
        ac.awaitSubmissionResult(1);

        ac.closeSession();
        lcs.shutdown();
    }
}
