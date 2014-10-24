package de.tuberlin.aura.demo.examples;


import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.datasets.AbstractDataset;
import de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;

import java.util.UUID;

public final class IterativeDataflowTest2 {

    // Disallow Instantiation.
    public IterativeDataflowTest2() {}

    // ---------------------------------------------------
    // UDFs.
    // ---------------------------------------------------

    public static final int COUNT = 1000000;

    public static final class Source1 extends SourceFunction<Tuple2<Integer, String>> {

        int count = COUNT;

        @Override
        public Tuple2<Integer, String> produce() {
            Tuple2<Integer, String> res = (--count >= 0 ) ?  new Tuple2<>(count, "String" + count) : null;
            if (count < 0) count = COUNT;
            return res;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<Integer, String>, Tuple2<Integer,String>> {

        @Override
        public Tuple2<Integer,String> map(final Tuple2<Integer, String> in) {
            return new Tuple2<>(in._1 + 1, in._2);
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
                        new TypeInformation(Integer.class),
                        new TypeInformation(String.class));

        final DataflowNodeProperties source1 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                "Source1",
                1,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                null,
                null,
                source1TypeInfo,
                Source1.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );

        final UUID dataset1UID = UUID.randomUUID();

        final DataflowNodeProperties dataset1 = new DataflowNodeProperties(
                dataset1UID,
                DataflowNodeProperties.DataflowNodeType.DATASET_REFERENCE,
                "Dataset1",
                4,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
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
                null
        );

        final DataflowNodeProperties map1 = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map1",
                4,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
                null,
                source1TypeInfo,
                Map1.class.getName(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );

        final UUID dataset2UID = UUID.randomUUID();

        final DataflowNodeProperties dataset2 = new DataflowNodeProperties(
                dataset2UID,
                DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                "Dataset2",
                4,
                1,
                new int[][] { source1TypeInfo.buildFieldSelectorChain("_1") },
                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                source1TypeInfo,
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
                dataset1UID
        );

        DataflowNodeProperties sink1 = new DataflowNodeProperties(
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
                null,
                null
        );

        // ---------------------------------------------------

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        // ---------------------------------------------------

        final Topology.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        atb1.addNode(new Topology.OperatorNode(source1))
                .connectTo("Dataset1", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.DatasetNode(dataset1));

        final Topology.AuraTopology topology1 = atb1.build("JOB1");
        ac.submitTopology(topology1, null);
        ac.awaitSubmissionResult(1);

        // ---------------------------------------------------

        final Topology.AuraTopologyBuilder atb2 = ac.createTopologyBuilder();
        atb2.addNode(new Topology.DatasetNode(dataset1, AbstractDataset.DatasetType.DATASET_ITERATION_HEAD_STATE))
                .connectTo("Map1", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.OperatorNode(map1))
                .connectTo("Dataset2", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.DatasetNode(dataset2, AbstractDataset.DatasetType.DATASET_ITERATION_TAIL_STATE));

        final Topology.AuraTopology topology2 = atb2.build("JOB2", true);
        ac.submitTopology(topology2, null);

        final int ITERATION_COUNT = 1500;

        for (int i = 0; i < ITERATION_COUNT; ++i) {

            ac.waitForIterationEnd(topology2.topologyID);
            ac.assignDataset(dataset1UID, dataset2UID);
            ac.reExecute(topology2.topologyID, i < ITERATION_COUNT - 1);
        }

        ac.awaitSubmissionResult(1);
        ac.assignDataset(dataset1UID, dataset2UID);

        // ---------------------------------------------------

        final Topology.AuraTopologyBuilder atb3 = ac.createTopologyBuilder();
        atb3.addNode(new Topology.DatasetNode(dataset1))
                .connectTo("Sink1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(sink1));

        final Topology.AuraTopology topology3 = atb3.build("JOB3");
        ac.submitTopology(topology3, null);
        ac.awaitSubmissionResult(1);

        // ---------------------------------------------------

        ac.closeSession();
        lcs.shutdown();

        // ---------------------------------------------------

        //ac.submitTopology(atb2.build("JOB2"), null);
        //ac.awaitSubmissionResult(1);
        //ac.assignDataset(dataset1UID, dataset2UID);


        /*Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();
        atb.addNode(new Topology.OperatorNode(source1))
                .connectTo("Map1", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.OperatorNode(map1))
                .connectTo("Sink1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(sink1));

        final Topology.AuraTopology topology = atb.build("ITERATIVE_JOB1", true);
        ac.submitTopology(topology, null);
        for (int i = 0; i < 5; ++i) {
            ac.waitForIterationEnd(topology.topologyID);
            ac.reExecute(topology.topologyID, i < 5 - 1);
        }
        ac.awaitSubmissionResult(1);*/

    }
}

