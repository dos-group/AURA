package de.tuberlin.aura.demo.examples;

import java.util.*;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.operators.impl.HDFSSinkPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.impl.HDFSSourcePhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.functions.FoldFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple4;
import de.tuberlin.aura.core.record.tuples.Tuple5;
import de.tuberlin.aura.core.topology.Topology;

public class KMeansExperiment {

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final Random random = new Random(123456789);

        IConfig simConfig = IConfigFactory.load(IConfig.Type.SIMULATOR);

        LocalClusterSimulator lcs = null;

        switch (simConfig.getString("simulator.mode")) {
            case "LOCAL":
                lcs = new LocalClusterSimulator(simConfig);
                break;
            case "cluster":
                break;
            default:
                lcs = new LocalClusterSimulator(simConfig);
        }

        final AuraClient auraClient = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        IConfig config = IConfigFactory.load(IConfig.Type.SIMULATOR);
        int executionUnits = config.getInt("simulator.tm.number") * config.getInt("tm.execution.units.number");

        final int solutionSetDop = Math.min(config.getInt("simulator.tm.number"), executionUnits / 4); // for evenly distribution of the big datasets
        final int operatorDop = solutionSetDop; // for having point-to-point connections (without partitioning)

        int iterationCount = 1;


        // initialize client values
        Double epsilon = 1e-4;
        Double change;

        // initialize centroids
        Collection<Tuple4<Long,Double,Double,Double>> centroids = new ArrayList<>();
        centroids.add(new Tuple4<>(0L, random.nextDouble(), random.nextDouble(), random.nextDouble()));
        centroids.add(new Tuple4<>(1L, random.nextDouble(), random.nextDouble(), random.nextDouble()));
        centroids.add(new Tuple4<>(2L, random.nextDouble(), random.nextDouble(), random.nextDouble()));

        final UUID centroidsBroadcastDatasetID = UUID.randomUUID();
        auraClient.broadcastDataset(centroidsBroadcastDatasetID, centroids);




        // JOB 1: assign all points to closest centroids

        final TypeInformation hdfsSourceInputTypeInfo = new TypeInformation(Tuple5.class,
                new TypeInformation(Long.class), // pointID
                new TypeInformation(Long.class), // clusterID
                new TypeInformation(Double.class), // x
                new TypeInformation(Double.class), // y
                new TypeInformation(Double.class)); // z

        Map<String,Object> srcConfig = new HashMap<>();
        srcConfig.put(HDFSSourcePhysicalOperator.HDFS_SOURCE_FILE_PATH, "/tmp/input/points");
        srcConfig.put(HDFSSourcePhysicalOperator.HDFS_SOURCE_INPUT_FIELD_TYPES, new Class<?>[] {Long.class, Long.class, Double.class, Double.class, Double.class});

        final DataflowNodeProperties sourceProperties =
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.HDFS_SOURCE,
                                "Source",
                                operatorDop,
                                1,
                                null,
                                null,
                                null,
                                null,
                                hdfsSourceInputTypeInfo,
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

        final TypeInformation pointsTuple4TypeInfo = new TypeInformation(Tuple4.class,
                new TypeInformation(Long.class), // pointID
                new TypeInformation(Double.class), // x
                new TypeInformation(Double.class), // y
                new TypeInformation(Double.class)); // z

        final DataflowNodeProperties tupleCutterMapProperties =
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                                "TupleCutterMap",
                                operatorDop,
                                1,
                                null,
                                null,
                                hdfsSourceInputTypeInfo,
                                null,
                                pointsTuple4TypeInfo,
                                TupleCutterMap.class.getName(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                        );

        final TypeInformation pointsTuple5TypeInfo = new TypeInformation(Tuple5.class,
                new TypeInformation(Long.class), // pointID
                new TypeInformation(Double.class), // x
                new TypeInformation(Double.class), // y
                new TypeInformation(Double.class), // z
                new TypeInformation(Long.class)); // clusterID

        final DataflowNodeProperties closestCentroidMapProperties =
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                                "ClosestCentroidMap",
                                operatorDop,
                                1,
                                null,
                                null,
                                pointsTuple4TypeInfo,
                                null,
                                pointsTuple5TypeInfo,
                                ClosestCentroidsMap.class.getName(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                Arrays.asList(centroidsBroadcastDatasetID),
                                null
                        );

        UUID solutionDatasetUID = UUID.randomUUID();
        String solutionDatasetName = "SolutionDataset" + iterationCount;

        DataflowNodeProperties solutionDatasetProperties =
                        new DataflowNodeProperties(
                                solutionDatasetUID,
                                DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                                solutionDatasetName,
                                solutionSetDop,
                                1,
                                new int[][] { pointsTuple5TypeInfo.buildFieldSelectorChain("_1") },
                                Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                pointsTuple5TypeInfo,
                                null,
                                pointsTuple5TypeInfo,
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

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.OperatorNode(Arrays.asList(sourceProperties, tupleCutterMapProperties, closestCentroidMapProperties))).
            connectTo(solutionDatasetName, Topology.Edge.TransferType.POINT_TO_POINT).
            addNode(new Topology.DatasetNode((solutionDatasetProperties)));

        Topology.AuraTopology initializeCentroids = atb.build("SETUP JOB (JOB 1) - assignToCentroids");

        auraClient.submitTopology(initializeCentroids, null);

        auraClient.awaitSubmissionResult(1);

        do {

            System.out.println();
            System.out.println("===== ITERATION COUNT : " + iterationCount);
            System.out.println();


            Topology.OperatorNode fold1Node = new Topology.OperatorNode(
                    new DataflowNodeProperties(
                            UUID.randomUUID(),
                            DataflowNodeProperties.DataflowNodeType.HASH_FOLD_OPERATOR,
                            "Fold1", operatorDop, 1,
                            new int[][] {pointsTuple5TypeInfo.buildFieldSelectorChain("_5")},
                            Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                            pointsTuple5TypeInfo,
                            null,
                            pointsTuple5TypeInfo,
                            LocalFold.class.getName(),
                            null, null, null, null,
                            new int[][] { pointsTuple5TypeInfo.buildFieldSelectorChain("_5") },
                            null, null, null
                    ));

            DataflowNodeProperties fold2Properties =
                    new DataflowNodeProperties(
                            UUID.randomUUID(),
                            DataflowNodeProperties.DataflowNodeType.HASH_FOLD_OPERATOR,
                            "Fold2", operatorDop, 1,
                            null,
                            null,
                            pointsTuple5TypeInfo,
                            null,
                            pointsTuple5TypeInfo,
                            GlobalFold.class.getName(),
                            null, null, null, null,
                            new int[][] { pointsTuple5TypeInfo.buildFieldSelectorChain("_5") },
                            null, null, null
                    );

            final DataflowNodeProperties newCentroidsMapProperties =
                            new DataflowNodeProperties(
                                    UUID.randomUUID(),
                                    DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                                    "NewCentroidsMap",
                                    operatorDop,
                                    1,
                                    null,
                                    null,
                                    pointsTuple5TypeInfo,
                                    null,
                                    pointsTuple4TypeInfo,
                                    NewCentroidsMap.class.getName(),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                            );

            final UUID iterationDatasetUID = UUID.randomUUID();
            final String iterationDatasetName = "IterationDataset" + iterationCount;

            final DataflowNodeProperties iterationDatasetProperties =
                            new DataflowNodeProperties(
                                    iterationDatasetUID,
                                    DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                                    iterationDatasetName,
                                    operatorDop,
                                    1,
                                    new int[][] { pointsTuple4TypeInfo.buildFieldSelectorChain("_1") },
                                    Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                                    pointsTuple4TypeInfo,
                                    null,
                                    pointsTuple4TypeInfo,
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

            Topology.AuraTopologyBuilder iterationAtb1 = auraClient.createTopologyBuilder();

            iterationAtb1.addNode(new Topology.DatasetNode(solutionDatasetProperties)).
                    connectTo("Fold1", Topology.Edge.TransferType.POINT_TO_POINT).
                    addNode(fold1Node).
                    connectTo("Fold2", Topology.Edge.TransferType.ALL_TO_ALL).
                    addNode(new Topology.OperatorNode(Arrays.asList(fold2Properties,newCentroidsMapProperties))).
                    connectTo(iterationDatasetName, Topology.Edge.TransferType.POINT_TO_POINT).
                    addNode(new Topology.DatasetNode((iterationDatasetProperties)));

            Topology.AuraTopology iterationJob1NewCentroids = iterationAtb1.build("First of the Iteration Jobs - newCentroids - ITERATION " + iterationCount);
            auraClient.submitTopology(iterationJob1NewCentroids, null);
            auraClient.awaitSubmissionResult(1);

            Collection<Tuple4<Long,Double,Double,Double>> changes = auraClient.getDataset(iterationDatasetUID);
            auraClient.eraseDataset(iterationDatasetUID);

            change = 0.0;

            for (Tuple4<Long,Double,Double,Double> centroid : centroids) {
                for (Tuple4<Long,Double,Double,Double> in : changes) {
                    if (centroid._1.equals(in._1)) {
                        change += ((in._2 - centroid._2) * (in._2 - centroid._2)) + ((in._3 - centroid._3) * (in._3 - centroid._3)) + ((in._4 - centroid._4) * (in._4 - centroid._4));
                    }
                }
            }

            for (Tuple4<Long,Double,Double,Double> centroid : centroids) {
                for (Tuple4<Long,Double,Double,Double> in : changes) {
                    if (centroid._1.equals(in._1)) {
                        centroid._2 = in._2;
                        centroid._3 = in._3;
                        centroid._4 = in._4;
                    }
                }
            }

            if (change > epsilon) {

                final UUID newCentroidsBroadcastDatasetID = UUID.randomUUID();
                auraClient.broadcastDataset(newCentroidsBroadcastDatasetID, centroids);

                final Topology.OperatorNode iterationClosestCentroidMapNode =
                        new Topology.OperatorNode(
                                new DataflowNodeProperties(
                                        UUID.randomUUID(),
                                        DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
                                        "IterationClosestCentroidMap",
                                        operatorDop,
                                        1,
                                        null,
                                        null,
                                        pointsTuple5TypeInfo,
                                        null,
                                        pointsTuple5TypeInfo,
                                        IterationClosestCentroidsMap.class.getName(),
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        Arrays.asList(newCentroidsBroadcastDatasetID),
                                        null
                                ));

                final UUID newSolutionDatasetUID = UUID.randomUUID();
                final String newSolutionDatasetName = "SolutionDataset" + (iterationCount + 1);

                DataflowNodeProperties newSolutionDatasetProperties = new DataflowNodeProperties(
                        newSolutionDatasetUID,
                        DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                        newSolutionDatasetName,
                        solutionSetDop,
                        1,
                        new int[][]{pointsTuple5TypeInfo.buildFieldSelectorChain("_1")},
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        pointsTuple5TypeInfo,
                        null,
                        pointsTuple5TypeInfo,
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

                Topology.AuraTopologyBuilder iterationAtb2 = auraClient.createTopologyBuilder();

                iterationAtb2.addNode(new Topology.DatasetNode(solutionDatasetProperties)).
                        connectTo("IterationClosestCentroidMap", Topology.Edge.TransferType.POINT_TO_POINT).
                        addNode(iterationClosestCentroidMapNode).
                        connectTo(newSolutionDatasetName, Topology.Edge.TransferType.POINT_TO_POINT).
                        addNode(new Topology.DatasetNode(newSolutionDatasetProperties));

                Topology.AuraTopology iterationJob2ClosestCentroids = iterationAtb2.build("Second of the Iteration Jobs - newCentroids - ITERATION " + iterationCount);
                auraClient.submitTopology(iterationJob2ClosestCentroids, null);
                auraClient.awaitSubmissionResult(1);

                auraClient.eraseDataset(solutionDatasetUID);

                solutionDatasetUID = newSolutionDatasetUID;
                solutionDatasetName = newSolutionDatasetName;
                solutionDatasetProperties = new DataflowNodeProperties(
                        solutionDatasetUID,
                        DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
                        solutionDatasetName,
                        operatorDop,
                        1,
                        new int[][] { pointsTuple5TypeInfo.buildFieldSelectorChain("_1") },
                        Partitioner.PartitioningStrategy.HASH_PARTITIONER,
                        pointsTuple5TypeInfo,
                        null,
                        pointsTuple5TypeInfo,
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

                iterationCount++;

            }

        } while(change > epsilon);



        // JOB 2: write all points to HDFS

        Map<String,Object> snkConfig = new HashMap<>();
        snkConfig.put(HDFSSinkPhysicalOperator.HDFS_SINK_FILE_PATH, "/tmp/output/points");

        Topology.OperatorNode sinkNode =
                new Topology.OperatorNode(
                        new DataflowNodeProperties(
                                UUID.randomUUID(),
                                DataflowNodeProperties.DataflowNodeType.HDFS_SINK,
                                "Sink",
                                operatorDop,
                                1,
                                null,
                                null,
                                pointsTuple5TypeInfo,
                                null,
                                null,
                                Sink.class.getName(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                snkConfig
                        ));

        Topology.AuraTopologyBuilder atb2 = auraClient.createTopologyBuilder();
        atb2.addNode(new Topology.DatasetNode(solutionDatasetProperties)).
            connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT).
            addNode(sinkNode);

        Topology.AuraTopology writeResults = atb2.build("WRITE OUT JOB - writeResults");
        auraClient.submitTopology(writeResults, null);
        auraClient.awaitSubmissionResult(1);

        auraClient.eraseDataset(solutionDatasetUID);

        auraClient.closeSession();

        if (lcs != null) {
            lcs.shutdown();
        }

        System.out.println();
        System.out.println("===== CENTROIDS : " + centroids);
        System.out.println("===== ITERATION COUNT : " + iterationCount);
        System.out.println();
    }

    // ---------------------------------------------------
    // UDFs.
    // ---------------------------------------------------

    public static final class TupleCutterMap extends MapFunction<Tuple5<Long,Long,Double,Double,Double>, Tuple4<Long,Double,Double,Double>> {

        @Override
        public Tuple4<Long,Double,Double,Double> map(final Tuple5<Long,Long,Double,Double,Double> in) {
            // strip the initial clusterID
            return new Tuple4<>(in._1, in._3, in._4, in._5);
        }
    }

    public static final class ClosestCentroidsMap extends MapFunction<Tuple4<Long,Double,Double,Double>, Tuple5<Long,Double,Double,Double,Long>> {

        private Collection<Tuple4<Long,Double,Double,Double>> centroids;

        @Override
        public void create() {
            final UUID dataset1 = getEnvironment().getProperties(2).broadcastVars.get(0);
            centroids = getEnvironment().getDataset(dataset1);
        }

        @Override
        public Tuple5<Long,Double,Double,Double,Long> map(final Tuple4<Long,Double,Double,Double> in) {

            Double minimalDistance = Double.MAX_VALUE;
            Long closestClusterID = null;

            for (Tuple4<Long,Double,Double,Double> centroid : centroids) {
                Double distance = ((in._2 - centroid._2) * (in._2 - centroid._2)) + ((in._3 - centroid._3) * (in._3 - centroid._3)) + ((in._4 - centroid._4) * (in._4 - centroid._4));

                if (distance < minimalDistance) {
                    minimalDistance = distance;
                    closestClusterID = centroid._1;
                }
            }

            return new Tuple5<>(in._1, in._2, in._3, in._4, closestClusterID);
        }
    }

    public static final class LocalFold extends FoldFunction<Tuple5<Long,Double,Double,Double,Long>,Tuple5<Long,Double,Double,Double,Long>> {

        @Override
        public Tuple5<Long,Double,Double,Double,Long> empty() {
            return new Tuple5<>(0L, 0.0, 0.0, 0.0, 0L);
        }

        @Override
        public Tuple5<Long,Double,Double,Double,Long> singleton(Tuple5<Long,Double,Double,Double,Long> element) {
            element._1 = 1L;
            return element;
        }

        @Override
        public Tuple5<Long,Double,Double,Double,Long> union(Tuple5<Long,Double,Double,Double,Long> result, Tuple5<Long,Double,Double,Double,Long> element) {

            // count elements per group
            result._1 = result._1 + element._1;

            // vector addition
            result._2 = result._2 + element._2;
            result._3 = result._3 + element._3;
            result._4 = result._4 + element._4;

            result._5 = element._5; // clusterID

            return result;
        }
    }

    public static final class GlobalFold extends FoldFunction<Tuple5<Long,Double,Double,Double,Long>,Tuple5<Long,Double,Double,Double,Long>> {

        @Override
        public Tuple5<Long,Double,Double,Double,Long> empty() {
            return new Tuple5<>(0L, 0.0, 0.0, 0.0, 0L);
        }

        @Override
        public Tuple5<Long,Double,Double,Double,Long> singleton(Tuple5<Long,Double,Double,Double,Long> element) {
            return element;
        }

        @Override
        public Tuple5<Long,Double,Double,Double,Long> union(Tuple5<Long,Double,Double,Double,Long> result, Tuple5<Long,Double,Double,Double,Long> element) {

            // count elements per group
            result._1 = result._1 + element._1;

            // vector addition
            result._2 = result._2 + element._2;
            result._3 = result._3 + element._3;
            result._4 = result._4 + element._4;

            result._5 = element._5; // clusterID

            return result;
        }
    }

    public static final class NewCentroidsMap extends MapFunction<Tuple5<Long,Double,Double,Double,Long>, Tuple4<Long,Double,Double,Double>> {

        @Override
        public Tuple4<Long,Double,Double,Double> map(final Tuple5<Long,Double,Double,Double,Long> in) {
            return new Tuple4<>(in._5, in._2 / in._1, in._3 / in._1, in._4 / in._1);
        }
    }

    public static final class IterationClosestCentroidsMap extends MapFunction<Tuple5<Long,Double,Double,Double,Long>, Tuple5<Long,Double,Double,Double,Long>> {

        private Collection<Tuple4<Long,Double,Double,Double>> centroids;

        @Override
        public void create() {
            final UUID dataset1 = getEnvironment().getProperties().broadcastVars.get(0);
            centroids = getEnvironment().getDataset(dataset1);
        }

        @Override
        public Tuple5<Long,Double,Double,Double,Long> map(final Tuple5<Long,Double,Double,Double,Long> in) {

            Double minimalDistance = Double.MAX_VALUE;
            Long closestClusterID = null;

            for (Tuple4<Long,Double,Double,Double> centroid : centroids) {
                Double distance = ((in._2 - centroid._2) * (in._2 - centroid._2)) + ((in._3 - centroid._3) * (in._3 - centroid._3)) + ((in._4 - centroid._4) * (in._4 - centroid._4));

                if (distance < minimalDistance) {
                    minimalDistance = distance;
                    closestClusterID = centroid._1;
                }
            }

            return new Tuple5<>(in._1, in._2, in._3, in._4, closestClusterID);
        }
    }

    public static final class Sink extends SinkFunction<Tuple5<Long,Double,Double,Double,Long>> {

        @Override
        public void consume(final Tuple5<Long,Double,Double,Double,Long> in) {
//            System.out.println(in);
        }
    }

}
