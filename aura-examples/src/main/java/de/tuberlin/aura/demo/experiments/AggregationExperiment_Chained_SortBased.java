package de.tuberlin.aura.demo.experiments;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.operators.impl.HDFSSinkPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.impl.HDFSSourcePhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.functions.FoldFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple3;
import de.tuberlin.aura.core.topology.Topology;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class AggregationExperiment_Chained_SortBased {

    // ---------------------------------------------------
    // UDFs.
    // ---------------------------------------------------

    /*public static final class UDFSource extends SourceFunction<Tuple3<Integer,String,Integer>> {

        private int count = 5500000;

        private final Random randInt = new Random();

        @Override
        public Tuple3<Integer, String, Integer> produce() {
            if (count-- > 0)
                return new Tuple3<>(randInt.nextInt(10000000), RandomStringUtils.random(10), randInt.nextInt(10000000));
            else
                return null;
        }
    }

    public static final class UDFSink extends SinkFunction<Tuple3<Integer,String,Integer>> {

        @Override
        public void consume(final Tuple3<Integer,String,Integer> in) {}
    }*/

    // ---------------------------------------------------

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
    // Type Information.
    // ---------------------------------------------------

    static final TypeInformation source1TypeInfo =
            new TypeInformation(Tuple3.class,
                    new TypeInformation(Integer.class), // key
                    new TypeInformation(String.class), // value
                    new TypeInformation(Integer.class)); // payload

    // ---------------------------------------------------
    // Operators.
    // ---------------------------------------------------

    private static Topology.OperatorNode createSourceSortGroupFold1Node(final int dop) {

        Map<String,Object> srcConfig = new HashMap<>();
        srcConfig.put(HDFSSourcePhysicalOperator.HDFS_SOURCE_FILE_PATH, "/tmp/input/groups");
        srcConfig.put(HDFSSourcePhysicalOperator.HDFS_SOURCE_INPUT_FIELD_TYPES, new Class<?>[] {Integer.class, String.class, Integer.class});

        DataflowNodeProperties sourceNodeProperties =
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
                );

        /*DataflowNodeProperties sourceNodeProperties =
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
                        UDFSource.class.getName(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                );*/

        DataflowNodeProperties sort1NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                        "Sort1",
                        dop,
                        1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        null,
                        null,
                        null,
                        new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        DataflowNodeProperties.SortOrder.ASCENDING,
                        null,
                        null,
                        null,
                        null
                );

        DataflowNodeProperties groupBy1NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                        "GroupBy1",
                        dop,
                        1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        null,
                        null,
                        null,
                        null,
                        null,
                        new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        null,
                        null,
                        null
                );

        DataflowNodeProperties fold1NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                        "Fold1",
                        dop,
                        1,
                        new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        MinValueFold.class.getName(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                );

        return new Topology.OperatorNode(
                Arrays.asList(
                        sourceNodeProperties,
                        sort1NodeProperties,
                        groupBy1NodeProperties,
                        fold1NodeProperties
                )
            );
    }

    private static Topology.OperatorNode createSortGroupFold2SinkNode(final int dop) {

        DataflowNodeProperties sort2NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                        "Sort2",
                        dop,
                        1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        null,
                        null,
                        null,
                        new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        DataflowNodeProperties.SortOrder.ASCENDING,
                        null,
                        null,
                        null,
                        null
                );

        DataflowNodeProperties groupBy2NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                        "GroupBy2",
                        dop,
                        1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        null,
                        null,
                        null,
                        null,
                        null,
                        new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                        null,
                        null,
                        null
                );

        DataflowNodeProperties fold2NodeProperties =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                        "Fold2",
                        dop,
                        1,
                        null,
                        null,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        MinValueFold.class.getName(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                );

        Map<String,Object> snkConfig = new HashMap<>();
        snkConfig.put(HDFSSinkPhysicalOperator.HDFS_SINK_FILE_PATH, "/tmp/output/groups");

        DataflowNodeProperties sinkNodeProperties =
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
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        snkConfig
                );

        /*DataflowNodeProperties sinkNodeProperties =
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
                        UDFSink.class.getName(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                );*/

        return new Topology.OperatorNode(
                Arrays.asList(sort2NodeProperties,
                        groupBy2NodeProperties,
                        fold2NodeProperties,
                        sinkNodeProperties)
                );
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        int dop = 80 / 2;

        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        try {

            // ---------------------------------------------------

            Topology.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
            atb1.addNode(createSourceSortGroupFold1Node(dop)).
                    connectTo("Sort2", Topology.Edge.TransferType.ALL_TO_ALL).
                    addNode(createSortGroupFold2SinkNode(dop));

            ac.submitTopology(atb1.build("JOB-AggregationExperiment_Chained_SortBased-1"), null);

            ac.awaitSubmissionResult(1);

            Thread.sleep(2000);

            // ---------------------------------------------------

            Topology.AuraTopologyBuilder atb2 = ac.createTopologyBuilder();
            atb2.addNode(createSourceSortGroupFold1Node(dop)).
                    connectTo("Sort2", Topology.Edge.TransferType.ALL_TO_ALL).
                    addNode(createSortGroupFold2SinkNode(dop));

            ac.submitTopology(atb2.build("JOB-AggregationExperiment_Chained_SortBased-2"), null);

            ac.awaitSubmissionResult(1);

            Thread.sleep(2000);

            // ---------------------------------------------------

            Topology.AuraTopologyBuilder atb3 = ac.createTopologyBuilder();
            atb3.addNode(createSourceSortGroupFold1Node(dop)).
                    connectTo("Sort2", Topology.Edge.TransferType.ALL_TO_ALL).
                    addNode(createSortGroupFold2SinkNode(dop));

            ac.submitTopology(atb3.build("JOB-AggregationExperiment_Chained_SortBased-3"), null);

            ac.awaitSubmissionResult(1);

            Thread.sleep(2000);

            // ---------------------------------------------------

            Topology.AuraTopologyBuilder atb4 = ac.createTopologyBuilder();
            atb4.addNode(createSourceSortGroupFold1Node(dop)).
                    connectTo("Sort2", Topology.Edge.TransferType.ALL_TO_ALL).
                    addNode(createSortGroupFold2SinkNode(dop));

            ac.submitTopology(atb4.build("JOB-AggregationExperiment_Chained_SortBased-4"), null);

            ac.awaitSubmissionResult(1);

            Thread.sleep(2000);

            // ---------------------------------------------------

            Topology.AuraTopologyBuilder atb5 = ac.createTopologyBuilder();
            atb5.addNode(createSourceSortGroupFold1Node(dop)).
                    connectTo("Sort2", Topology.Edge.TransferType.ALL_TO_ALL).
                    addNode(createSortGroupFold2SinkNode(dop));

            ac.submitTopology(atb5.build("JOB-AggregationExperiment_Chained_SortBased-5"), null);

            ac.awaitSubmissionResult(1);

            Thread.sleep(2000);

            // ---------------------------------------------------

            Topology.AuraTopologyBuilder atb6 = ac.createTopologyBuilder();
            atb6.addNode(createSourceSortGroupFold1Node(dop)).
                    connectTo("Sort2", Topology.Edge.TransferType.ALL_TO_ALL).
                    addNode(createSortGroupFold2SinkNode(dop));

            ac.submitTopology(atb6.build("JOB-AggregationExperiment_Chained_SortBased-6"), null);

            ac.awaitSubmissionResult(1);

            Thread.sleep(2000);

            // ---------------------------------------------------

            Topology.AuraTopologyBuilder atb7 = ac.createTopologyBuilder();
            atb7.addNode(createSourceSortGroupFold1Node(dop)).
                    connectTo("Sort2", Topology.Edge.TransferType.ALL_TO_ALL).
                    addNode(createSortGroupFold2SinkNode(dop));

            ac.submitTopology(atb7.build("JOB-AggregationExperiment_Chained_SortBased-7"), null);

            ac.awaitSubmissionResult(1);

            // ---------------------------------------------------

        } catch (InterruptedException ie) {
            throw new IllegalStateException(ie);
        }

        ac.closeSession();
    }
}
