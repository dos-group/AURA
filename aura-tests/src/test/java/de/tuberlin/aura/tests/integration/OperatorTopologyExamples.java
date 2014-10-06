package de.tuberlin.aura.tests.integration;

import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.*;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;

public class OperatorTopologyExamples {

    // --------------------------------------------------
    // Topology Examples.
    // --------------------------------------------------

    public static Topology.AuraTopology testJob1(AuraClient ac, int executionUnits) {

        int dop = executionUnits / 6;

        Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();
        atb.addNode(new Topology.OperatorNode(source1DataflowNodeProperties(dop)), Source1.class).
                connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map1DataflowNodeProperties(dop)), Map1.class).
                connectTo("FlatMap1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(flatMap1DataflowNodeProperties(dop)), FlatMap1.class).
                connectTo("Filter1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(filter1DataflowNodeProperties(dop)), Filter1.class).
                connectTo("Fold1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(fold1DataflowNodeProperties(dop)), Fold1.class).
                connectTo("Sink1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(sink1DataflowNodeProperties()), Sink1.class);

        return atb.build("JOB1");
    }

    public static Topology.AuraTopology testJob2(AuraClient ac, int executionUnits) {

        int dop = executionUnits / 8;

        Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source1DataflowNodeProperties(dop)), Source1.class).
                connectTo("Join1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(source2DataflowNodeProperties(dop)), Source2.class).
                connectTo("Difference1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(source3DataflowNodeProperties(dop)), Source3.class).
                connectTo("Difference1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(difference1DataflowNodeProperties(dop))).
                connectTo("Join1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(join1DataflowNodeProperties(dop))).
                connectTo("Distinct1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(distinct1DataflowNodeProperties(dop))).
                connectTo("Sort1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(sort1JoinTypesDataflowNodeProperties(dop))).
                connectTo("Sink2", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(sink2DataflowNodeProperties()), JoinSink1.class);

        return atb.build("JOB2");
    }

    public static Topology.AuraTopology testJob3(AuraClient ac, int executionUnits) {

        int dop = executionUnits / 5;

        Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source4DataflowNodeProperties(dop)), Source4.class).
                connectTo("Sort1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(sort1SourceTypeDataflowNodeProperties(dop))).
                connectTo("GroupBy1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(groupBy1DataflowNodeProperties(dop))).
                connectTo("Fold1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(fold1GroupTypeDataflowNodeProperties(dop)), Fold1.class).
                connectTo("Sink1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(sink1DataflowNodeProperties()), Sink1.class);

        return atb.build("JOB3");
    }

    public static Topology.AuraTopology testJob4(AuraClient ac, int executionUnits) {

        int dop = executionUnits / 6;

        Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source4DataflowNodeProperties(dop)), Source4.class).
                connectTo("Sort1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(sort1SourceTypeDataflowNodeProperties(dop))).
                connectTo("GroupBy1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(groupBy1DataflowNodeProperties(dop))).
                connectTo("MapGroup1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(mapGroup1DataflowNodeProperties(dop)), GroupMap1.class).
                connectTo("Fold1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(fold1GroupTypeDataflowNodeProperties(dop)), Fold1.class).
                connectTo("Sink1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.OperatorNode(sink1DataflowNodeProperties()), Sink1.class);

        return atb.build("JOB4");
    }

    // ---------------------------------------------------
    // Types.
    // ---------------------------------------------------

    private static TypeInformation source1TypeInfo() {
        return new TypeInformation(Tuple2.class,
                new TypeInformation(String.class),
                new TypeInformation(Integer.class));
    }

    private static TypeInformation join1TypeInfo(TypeInformation joinedType1, TypeInformation joinedType2) {
        return new TypeInformation(Tuple2.class,
                joinedType1,
                joinedType2);
    }

    private static TypeInformation groupBy1TypeInfo() {
        return new TypeInformation(Tuple2.class, true,
                new TypeInformation(String.class),
                new TypeInformation(Integer.class));
    }

    // ---------------------------------------------------
    // Dataflow Nodes.
    // ---------------------------------------------------

    private static DataflowNodeProperties source1DataflowNodeProperties(int source1dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source1",
                source1dop,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Source1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties source2DataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source2", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Source2.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties source3DataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source3", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Source3.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties source4DataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source4", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Source4.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties sink1DataflowNodeProperties() {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                "Sink1", 1, 1,
                null,
                null,
                source1TypeInfo,
                null,
                null,
                Sink1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties sink2DataflowNodeProperties() {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SINK,
                "Sink2", 1, 1,
                null,
                null,
                join1TypeInfo,
                null,
                null,
                JoinSink1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties map1DataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map1", dop, 1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                Map1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties flatMap1DataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.FLAT_MAP_TUPLE_OPERATOR,
                "FlatMap1", dop, 1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                FlatMap1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties filter1DataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.FILTER_OPERATOR,
                "Filter1", dop, 1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                Filter1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties fold1DataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                "Fold1", dop, 1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                Fold1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties sort1JoinTypesDataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                "Sort1", dop, 1,
                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                join1TypeInfo,
                null,
                join1TypeInfo,
                null,
                null, null, new int[][] { join1TypeInfo.buildFieldSelectorChain("_2._2") }, DataflowNodeProperties.SortOrder.DESCENDING, null,
                null, null
        );
    }

    private static DataflowNodeProperties distinct1DataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.DISTINCT_OPERATOR,
                "Distinct1", dop, 1,
                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                join1TypeInfo,
                null,
                join1TypeInfo,
                null,
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties join1DataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation join1TypeInfo = join1TypeInfo(source1TypeInfo, source1TypeInfo);

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.HASH_JOIN_OPERATOR,
                "Join1", dop, 1,
                new int[][] { join1TypeInfo.buildFieldSelectorChain("_1._2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                source1TypeInfo,
                join1TypeInfo,
                null,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") }, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") }, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties difference1DataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.DIFFERENCE_OPERATOR,
                "Difference1", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                source1TypeInfo,
                source1TypeInfo,
                null,
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties groupBy1DataflowNodeProperties(int dop) {

        TypeInformation source1TypeInfo = source1TypeInfo();
        TypeInformation groupBy1TypeInfo = groupBy1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.GROUP_BY_OPERATOR,
                "GroupBy1", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                groupBy1TypeInfo,
                null,
                null, null, null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                null, null
        );
    }

    private static DataflowNodeProperties fold1GroupTypeDataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation groupBy1TypeInfo = groupBy1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.FOLD_OPERATOR,
                "Fold1", dop, 1,
                new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                groupBy1TypeInfo,
                null,
                source1TypeInfo,
                Fold1.class.getName(),
                null, null, null, null, null,
                null, null
        );
    }

    private static DataflowNodeProperties sort1SourceTypeDataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.SORT_OPERATOR,
                "Sort1", dop, 1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                null,
                null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") }, DataflowNodeProperties.SortOrder.ASCENDING, null,
                null, null
        );
    }

    private static DataflowNodeProperties mapGroup1DataflowNodeProperties(int dop) {

        final TypeInformation source1TypeInfo = source1TypeInfo();
        final TypeInformation groupBy1TypeInfo = groupBy1TypeInfo();

        return new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_GROUP_OPERATOR,
                "MapGroup1", dop, 1,
                null,
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                groupBy1TypeInfo,
                null,
                groupBy1TypeInfo,
                GroupMap1.class.getName(),
                null, null, null, null, new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
                null, null
        );
    }

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
            return new Tuple2<>("HELLO", in._2);
        }
    }

    public static final class FlatMap1 extends FlatMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public void flatMap(Tuple2<String,Integer> in, Collection<Tuple2<String,Integer>> c) {
            if ((in._2 % 10) == 0) {
                c.add(new Tuple2<>("HEL", in._2));
                c.add(new Tuple2<>("LO", in._2 + 1));
            }
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

    public static final class Filter1 extends FilterFunction<Tuple2<String,Integer>> {

        @Override
        public boolean filter(final Tuple2<String,Integer> in) {
            return in._2 % 2 == 0;
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

}
