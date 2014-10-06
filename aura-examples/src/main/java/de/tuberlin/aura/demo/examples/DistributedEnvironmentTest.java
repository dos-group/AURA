package de.tuberlin.aura.demo.examples;

import java.util.*;

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

public class DistributedEnvironmentTest {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DistributedEnvironmentTest.class);

    // ---------------------------------------------------
    // User-defined Functions.
    // ---------------------------------------------------

    public static final class Source1 extends SourceFunction<Tuple2<String,Integer>> {

        int count = 1000;

        @Override
        public  Tuple2<String,Integer> produce() {
            return (--count >= 0 ) ?  new Tuple2<>("SOURCE1", count) : null;
        }
    }

    public static final class Map1 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            return new Tuple2<>("HELLO", in._2);
        }
    }

    public static final class Map2 extends MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>> {

        private Collection<Tuple2<String,Integer>> broadcastDataset;

        @Override
        public void create() {
            final UUID dataset1 = getEnvironment().getProperties().broadcastVars.get(0);
            broadcastDataset = getEnvironment().getDataset(dataset1);
        }

        @Override
        public Tuple2<String,Integer> map(final Tuple2<String,Integer> in) {
            final StringBuilder sb = new StringBuilder();
            for(final Tuple2<String,Integer> t : broadcastDataset)
                sb.append(t._1);
            return new Tuple2<>("HELLO" + sb.toString(), in._2);
        }
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        LocalClusterSimulator clusterSimulator = null;

        IConfig simConfig = IConfigFactory.load(IConfig.Type.SIMULATOR);
        switch (simConfig.getString("simulator.mode")) {
            case "LOCAL":
                clusterSimulator = new LocalClusterSimulator(simConfig);
                break;
            case "cluster":
                break;
            default:
                LOG.warn("'simulator mode' has unknown value. Fallback to LOCAL mode.");
                clusterSimulator = new LocalClusterSimulator(simConfig);
        }

        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        int nodes = simConfig.getInt("simulator.tm.number");
        int cores = simConfig.getInt("tm.execution.units.number");

        int executionUnits = nodes * cores;

        final TypeInformation source1TypeInfo =
                new TypeInformation(Tuple2.class,
                        new TypeInformation(String.class),
                        new TypeInformation(Integer.class));

        final DataflowNodeProperties source1 =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
                        "Source1",
                        executionUnits / 3,
                        1,
                        new int[][] { source1TypeInfo.buildFieldSelectorChain("_2") },
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

        final DataflowNodeProperties map1 =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                        "Map1",
                        executionUnits / 3,
                        1,
                        new int[][] {source1TypeInfo.buildFieldSelectorChain("_2")},
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

        final UUID dataset1UID = UUID.randomUUID();

        final DataflowNodeProperties dataset1Properties = new DataflowNodeProperties(
                dataset1UID,
                DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                "Dataset1",
                executionUnits / 3,
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
                null
        );

        Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();

        atb.addNode(new Topology.OperatorNode(source1), Source1.class).
                connectTo("Map1", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map1), Map1.class).
                connectTo("Dataset1", Topology.Edge.TransferType.ALL_TO_ALL).
                addNode(new Topology.DatasetNode(dataset1Properties));

        final Topology.AuraTopology topology1 = atb.build("JOB1");
        ac.submitTopology(topology1, null);
        ac.awaitSubmissionResult(1);

        final Collection<Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>>> collection1 = ac.getDataset(dataset1UID);
        for (Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>> tuple : collection1)
                System.out.println(tuple);

        System.out.println("----------------------------------------------------");

        Collection<Tuple2<String,Integer>> broadcastDataset = new ArrayList<>();
        broadcastDataset.add(new Tuple2<>("A", 0));
        broadcastDataset.add(new Tuple2<>("B", 1));
        broadcastDataset.add(new Tuple2<>("C", 2));
        broadcastDataset.add(new Tuple2<>("D", 3));
        final UUID broadcastDatasetID = UUID.randomUUID();
        ac.broadcastDataset(broadcastDatasetID, broadcastDataset);

        final DataflowNodeProperties map2 =
                new DataflowNodeProperties(
                        UUID.randomUUID(),
                        DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                        "Map2",
                        executionUnits / 3,
                        1,
                        new int[][] {source1TypeInfo.buildFieldSelectorChain("_1")},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        source1TypeInfo,
                        null,
                        source1TypeInfo,
                        Map2.class.getName(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        Arrays.asList(broadcastDatasetID)
                );

        final UUID dataset2UID = UUID.randomUUID();

        final DataflowNodeProperties dataset2Properties = new DataflowNodeProperties(
                dataset2UID,
                DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                "Dataset2",
                executionUnits / 3,
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
                null
        );

        Topology.AuraTopologyBuilder atb2 = ac.createTopologyBuilder();

        atb2.addNode(new Topology.DatasetNode(dataset1Properties)).
                connectTo("Map2", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.OperatorNode(map2), Map2.class).
                connectTo("Dataset2", Topology.Edge.TransferType.POINT_TO_POINT).
                addNode(new Topology.DatasetNode(dataset2Properties));

        final Topology.AuraTopology topology2 = atb2.build("JOB2");
        ac.submitTopology(topology2, null);
        ac.awaitSubmissionResult(1);

        final Collection<Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>>> collection2 = ac.getDataset(dataset2UID);
        for (Tuple2<Tuple2<String,Integer>, Tuple2<String,Integer>> tuple : collection2)
            System.out.println(tuple);

        ac.closeSession();

        if (clusterSimulator != null) {
            clusterSimulator.shutdown();
        }
    }
}
