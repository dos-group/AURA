package de.tuberlin.aura.demo.examples;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.FoldFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple3;
import de.tuberlin.aura.core.topology.Topology;

import java.util.Arrays;
import java.util.UUID;


public class Test1 {

    // ---------------------------------------------------
    // UDFs.
    // ---------------------------------------------------

    public static final class UDFSource1 extends SourceFunction<Tuple3<Integer,String,Integer>> {

        int count = 25000000;

        @Override
        public Tuple3<Integer, String, Integer> produce() {

            if (count == 0)
                return null;

            if ((count-- % 2) == 0) {
                return new Tuple3<>(1,"eins",count);
            } else {
                return new Tuple3<>(2,"zwei",count);
            }
        }
    }

    public static final class Sink1 extends SinkFunction<Tuple3<Integer,String,Integer>> {

        @Override
        public void consume(final Tuple3<Integer,String,Integer> in) {
//            System.out.println(in);
        }
    }

    public static final class MinValueFold extends FoldFunction<Tuple3<Integer,String,Integer>,Tuple3<Integer,String,Integer>> {

        @Override
        public Tuple3<Integer,String,Integer> empty() {
            return new Tuple3<>(0, "", Integer.MAX_VALUE);
        }

        @Override
        public Tuple3<Integer,String,Integer> singleton(Tuple3<Integer,String,Integer> element) {
            return element;
        }

        @Override
        public Tuple3<Integer,String,Integer> union(Tuple3<Integer,String,Integer> minimalValueSoFar, Tuple3<Integer,String,Integer> element) {

            return element._3 < minimalValueSoFar._3 ? element : minimalValueSoFar;
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple3.class,
                        new TypeInformation(Integer.class), // key
                        new TypeInformation(String.class), // value
                        new TypeInformation(Integer.class)); // payload

//        int dop = 40 / 4;
        int dop = 16 / 4;

        final Topology.OperatorNode sourceNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                                "Source",
                                dop,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                null,
                                null,
                                source1TypeInfo,
                                UDFSource1.class.getName(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                        ));

        /*Map<String,Object> srcConfig = new HashMap<>();
        srcConfig.put(HDFSSourcePhysicalOperator.HDFS_SOURCE_FILE_PATH, "/tmp/input/groups");
        srcConfig.put(HDFSSourcePhysicalOperator.HDFS_SOURCE_INPUT_FIELD_TYPES, new Class<?>[] {Integer.class, String.class, Integer.class});

        final Topology.OperatorNode sourceNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.HDFS_SOURCE,
                                "Source",
                                dop,
                                1,
                                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                null,
                                null,
                                source1TypeInfo,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                srcConfig
                        ));*/

//        Topology.OperatorNode fold1Node = new Topology.OperatorNode(
//                new DataflowNodeProperties(
//                        UUID.randomUUID(),
//                        DataflowNodeProperties.DataflowNodeType.HASH_FOLD_OPERATOR,
//                        "Fold1", dop, 1,
//                        new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
//                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
//                        source1TypeInfo,
//                        null,
//                        source1TypeInfo,
//                        MinValueFold.class.getName(),
//                        null, null, null, null,
//                        new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
//                        null, null, null
//                ));
//
//        Topology.OperatorNode fold2Node = new Topology.OperatorNode(
//                new DataflowNodeProperties(
//                        UUID.randomUUID(),
//                        DataflowNodeProperties.DataflowNodeType.HASH_FOLD_OPERATOR,
//                        "Fold2", dop, 1,
//                        null,
//                        null,
//                        source1TypeInfo,
//                        null,
//                        source1TypeInfo,
//                        MinValueFold.class.getName(),
//                        null, null, null, null,
//                        new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
//                        null, null, null
//                ));

        DataflowNodeProperties sort1NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                        "Sort1", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        null,
                        null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        DataflowNodeProperties.SortOrder.ASCENDING, null,
                        null, null, null
                );

        DataflowNodeProperties groupBy1NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                        "GroupBy1", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        null,
                        null, null, null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        null, null, null
                );

        DataflowNodeProperties fold1NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                        "Fold1", dop, 1,
                        new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        MinValueFold.class.getName(),
                        null, null, null, null, null,
                        null, null, null
                );

        Topology.OperatorNode chainedSortGroupFold1Node = new Topology.OperatorNode(Arrays.asList(sort1NodeProperties,
                groupBy1NodeProperties,
                fold1NodeProperties));

        DataflowNodeProperties sort2NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                        "Sort2", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        null,
                        null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        DataflowNodeProperties.SortOrder.ASCENDING, null,
                        null, null, null
                );

        DataflowNodeProperties groupBy2NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                        "GroupBy2", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        null,
                        null, null, null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        null, null, null
                );

        DataflowNodeProperties fold2NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                        "Fold2", dop, 1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        MinValueFold.class.getName(),
                        null, null, null, null, null,
                        null, null, null
                );

        Topology.OperatorNode chainedSortGroupFold2Node = new Topology.OperatorNode(Arrays.asList(sort2NodeProperties,
                groupBy2NodeProperties,
                fold2NodeProperties));

        Topology.OperatorNode sinkNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                                "Sink",
                                dop,
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
                        ));

        /*Map<String,Object> snkConfig = new HashMap<>();
        snkConfig.put(HDFSSinkPhysicalOperator.HDFS_SINK_FILE_PATH, "/tmp/output/groups");

        Topology.OperatorNode sinkNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.HDFS_SINK,
                                "Sink",
                                dop,
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
                                snkConfig
                        ));*/

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();
        atb.addNode(sourceNode).
                connectTo("Sort1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(chainedSortGroupFold1Node).
                connectTo("Sort2", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(chainedSortGroupFold2Node).
                connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(sinkNode);

        ac.submitTopology(atb.build("JOB1"), null);

        ac.awaitSubmissionResult(1);
        ac.closeSession();
        lcs.shutdown();
    }
}