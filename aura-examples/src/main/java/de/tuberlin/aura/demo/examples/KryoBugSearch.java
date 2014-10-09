package de.tuberlin.aura.demo.examples;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowAPI;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.generator.TopologyGenerator;
import de.tuberlin.aura.core.dataflow.udfs.functions.FoldFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.GroupMapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;

import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

/**
 *
 */
public class KryoBugSearch {


    public static final class Source4 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 100000;

        Random rand = new Random(13454);

        @Override
        public Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("RIGHT_SOURCE", rand.nextInt(100)) : null;
        }
    }

    public static final class GroupMap1 extends GroupMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public void map(Iterator<Tuple2<String,Integer>> in, Collection<Tuple2<String,Integer>> output) {

            Integer count = 0;

            while (in.hasNext()) {
                Tuple2<String,Integer> t = in.next();
                output.add(new Tuple2<>(t._1, t._2 + count++));
            }
        }

    }

    public static final class Fold1 extends FoldFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> initialValue() {
            return new Tuple2<>("RESULT", 0);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> in) {
            return new Tuple2<>(in._1, 1);
        }

        @Override
        public Tuple2<String,Integer> add(Tuple2<String,Integer> currentValue, Tuple2<String, Integer> in) {
            return new Tuple2<>("RESULT", currentValue._2 + in._2);
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

        final DataflowAPI.DataflowNodeDescriptor source4 =
                new DataflowAPI.DataflowNodeDescriptor(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source4",
                                1,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                null,
                                null,
                                source1TypeInfo,
                                Source4.class.getName(),
                                null,
                                null,
                                null,
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
                                "Sort1",
                                1,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                null,
                                source1TypeInfo,
                                null,
                                null,
                                null,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                                DataflowNodeProperties.SortOrder.ASCENDING, null,
                                null,
                                null,
                                null
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
                                "GroupBy1",
                                1,
                                1,
                                null,
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                source1TypeInfo,
                                null,
                                groupBy1TypeInfo,
                                null,
                                null,
                                null,
                                null,
                                null,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
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
                                "MapGroup1",
                                1,
                                1,
                                null,
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                groupBy1TypeInfo,
                                null,
                                groupBy1TypeInfo,
                                GroupMap1.class.getName(),
                                null,
                                null,
                                null,
                                null,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
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
                                "Fold1",
                                1,
                                1,
                                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                groupBy1TypeInfo,
                                null,
                                source1TypeInfo,
                                Fold1.class.getName(),
                                null,
                                null,
                                null,
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
                        ),
                        fold1
                );

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        //Topology.AuraTopology topology = new TopologyGenerator(ac.createTopologyBuilder()).generate(sink1).toTopology("JOB4");

        //ac.submitTopology(topology, null);




        ac.awaitSubmissionResult(1);
        ac.closeSession();
        lcs.shutdown();
    }
}
