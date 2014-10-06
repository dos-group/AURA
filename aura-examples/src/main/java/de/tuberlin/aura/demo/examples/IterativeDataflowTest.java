package de.tuberlin.aura.demo.examples;


import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.topology.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public final class IterativeDataflowTest {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(IterativeDataflowTest.class);

    // Disallow Instantiation.
    public IterativeDataflowTest() {}

    // ---------------------------------------------------
    // UDFs.
    // ---------------------------------------------------

    public static final class Source1 extends SourceFunction<Tuple2<Integer, String>> {

        int count = 1000;

        @Override
        public Tuple2<Integer, String> produce() {
            return (--count >= 0 ) ?  new Tuple2<>(count, "String" + count) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<Integer, String>, Tuple2<Integer,String>> {

        @Override
        public Tuple2<Integer,String> map(final Tuple2<Integer, String> in) {
            return in;
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
                "Source1", 1, 1,
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
                null
        );


        final UUID dataset1UID = UUID.randomUUID();

        final DataflowNodeProperties dataset1 = new DataflowNodeProperties(
                dataset1UID,
                DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                "Dataset1", 1, 1,
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
                null
        );


        final DataflowNodeProperties map = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                "Map1", 1, 1,
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
                null
        );


        final DataflowNodeProperties loopControl = new DataflowNodeProperties(
                UUID.randomUUID(),
                DataflowNodeProperties.DataflowNodeType.LOOP_CONTROL_OPERATOR,
                "LoopControl1", 1, 1,
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
                null
        );


        LocalClusterSimulator clusterSimulator = null;

        IConfig simConfig = IConfigFactory.load(IConfig.Type.SIMULATOR);
        switch (simConfig.getString("simulator.mode")) {
            case "LOCAL":
                new LocalClusterSimulator(simConfig);
                break;
            case "cluster":
                break;
            default:
                LOG.warn("'simulator mode' has unknown value. Fallback to LOCAL mode.");
                new LocalClusterSimulator(simConfig);
        }

        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();
        atb.addNode(new Topology.OperatorNode(source1), Source1.class)
                .connectTo("Dataset1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.DatasetNode(dataset1))
                .connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(map), Map1.class)
                .connectTo("LoopControl1", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.OperatorNode(loopControl))
                .connectTo("Dataset1", Topology.Edge.TransferType.POINT_TO_POINT);

        ac.submitTopology(atb.build("JOB1"), null);
        ac.awaitSubmissionResult(1);
        ac.closeSession();

        if (clusterSimulator != null) {
            clusterSimulator.shutdown();
        }

    }
}

