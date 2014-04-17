package de.tuberlin.aura.demo.client;

import java.io.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryManager;
import de.tuberlin.aura.core.statistic.*;
import de.tuberlin.aura.core.task.common.*;
import de.tuberlin.aura.core.task.common.BenchmarkRecord.WordCountBenchmarkRecord;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopologyBuilder;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Edge;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Node;

public final class MapReduceClient {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MapReduceClient.class);

    // Disallow Instantiation.
    private MapReduceClient() {}

    /**
*
*/
    public static class Mapper extends TaskInvokeable {

        private static final String DATA_PATH = "/data/chwuertz/aura/input/";

        // private static final String DATA_PATH = "/home/teots/Desktop/text";

        public Mapper(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driverContext.taskDescriptor.taskID;

            long send = 0l;
            long accumulatedBufferSize = 0l;

            final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);

            File dir = new File(DATA_PATH);
            File[] files = dir.listFiles();
            File dataFile = files[driverContext.taskDescriptor.taskIndex % files.length];
            driverContext.measurementManager.add(new InformationMeasurement(MeasurementType.INFORMATION, "FILE", dataFile.getName()));

            Map<Integer, Integer> indexMap = new TreeMap<>();
            for (int i = 0; i < outputs.size(); ++i) {
                indexMap.put(i, 0);
            }

            BufferedReader br = new BufferedReader(new FileReader(dataFile));
            String line = null;
            int index = -1;

            while ((line = br.readLine()) != null && isInvokeableRunning()) {
                String[] tokens = line.split(" ");
                for (String word : tokens) {
                    // Find destination task.

                    // Hash partitioning
                    // int index = word.hashCode() % outputs.size();
                    // if (index < 0) {
                    // index += outputs.size();
                    // }

                    // Round-Robin
                    index = ++index % outputs.size();

                    indexMap.put(index, indexMap.get(index) + 1);

                    final UUID outputTaskID = getTaskID(0, index);

                    final MemoryManager.MemoryView buffer = producer.alloc();
                    final IOEvents.TransferBufferEvent event = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);

                    final Record<WordCountBenchmarkRecord> record = new Record<>(new WordCountBenchmarkRecord(word, 1));

                    driverContext.recordWriter.writeRecord(record, event);
                    producer.emit(0, index, event);

                    accumulatedBufferSize += buffer.size();
                    ++send;
                }
            }

            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "SOURCE", send));
            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "TOTAL_BUFFER_SIZE", accumulatedBufferSize));

            for (Map.Entry<Integer, Integer> entry : indexMap.entrySet()) {
                driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER,
                                                                           "INDEX " + Integer.toString(entry.getKey()),
                                                                           entry.getValue()));
            }
        }

        @Override
        public void close() throws Throwable {
            producer.done();
        }
    }

    /**
     *
     */
    public static class Reducer extends TaskInvokeable {

        public Reducer(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {
            Map<Long, Long> dataArrival = new HashMap<>();
            List<Long> latencies = new LinkedList<>();

            long received = 0l;
            long accumulatedBufferSize = 0l;

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent event = consumer.absorb(0);

                // LOG.debug("received: " + buffer);

                if (event != null) {
                    final Record<BenchmarkRecord> record = driverContext.recordReader.readRecord(event);

                    long now = System.currentTimeMillis();
                    long latency = now - record.getData().time;
                    latencies.add(latency);
                    if (!dataArrival.containsKey(now / 1000l)) {
                        dataArrival.put(now / 1000l, 0l);
                    }
                    dataArrival.put(now / 1000l, dataArrival.get(now / 1000l) + new Long(event.buffer.size()));

                    accumulatedBufferSize += event.buffer.size();
                    ++received;

                    event.buffer.free();
                }
            }

            long latencySum = 0l;
            long minLatency = Long.MAX_VALUE;
            long maxLatency = Long.MIN_VALUE;

            for (long latency : latencies) {
                latencySum += latency;

                if (latency < minLatency) {
                    minLatency = latency;
                }

                if (latency > maxLatency) {
                    maxLatency = latency;
                }
            }

            double avgLatency = (double) latencySum / (double) latencies.size();
            long medianLatency = MedianHelper.findMedian(latencies);

            // LOG.info("RESULTS|" + Double.toString(avgLatency) + "|" + Long.toString(minLatency) +
            // "|" + Long.toString(maxLatency) + "|"
            // + Long.toString(medianLatency));
            driverContext.measurementManager.add(new AccumulatedLatencyMeasurement(MeasurementType.LATENCY,
                                                                                   "Buffer latency",
                                                                                   minLatency,
                                                                                   maxLatency,
                                                                                   avgLatency,
                                                                                   medianLatency));
            for (Map.Entry<Long, Long> entry : dataArrival.entrySet()) {
                driverContext.measurementManager.add(new ThroughputMeasurement(MeasurementType.THROUGHPUT,
                                                                               "Throughput",
                                                                               entry.getValue(),
                                                                               entry.getKey()));
            }

            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "SINK", received));
            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "TOTAL_BUFFER_SIZE", accumulatedBufferSize));
        }
    }


    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        // final SimpleLayout layout = new SimpleLayout();
        // final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
        // LOG.addAppender(consoleAppender);
        // LOG.setLevel(Level.INFO);

        // Local
        // final String measurementPath = "/home/teots/Desktop/measurements";
        // final String zookeeperAddress = "localhost:2181";
        // final LocalClusterSimulator lcs =
        // new
        // LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
        // true,
        // zookeeperAddress,
        // 6,
        // measurementPath);

        // Wally
        final String zookeeperAddress = "wally001.cit.tu-berlin.de:2181";

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        atb1.addNode(new Node(UUID.randomUUID(), "Mapper", 99, 1), Mapper.class)
            .connectTo("Reducer", Edge.TransferType.ALL_TO_ALL)
            .addNode(new Node(UUID.randomUUID(), "Reducer", 99, 1), Reducer.class);

        // Add the job resubmission handler.
        ac.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, new EventHandler() {

            private int runs = 1;

            private int jobCounter = 1;

            @Handle(event = IOEvents.ControlIOEvent.class, type = IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED)
            private void handleTopologyFinished(final IOEvents.ControlIOEvent event) {
                String jobName = (String) event.getPayload();
                LOG.info("Topology ({}) finished.", jobName);

                if (jobCounter < runs) {
                    Thread t = new Thread() {

                        public void run() {
                            // This break is only necessary to make it easier to distinguish jobs in
                            // the log files.
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            AuraTopology at =
                                    atb1.build("Job " + Integer.toString(++jobCounter), EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING));
                            ac.submitTopology(at, null);
                        }
                    };

                    t.start();
                }
            }
        });

        // Submit the first job. Other runs of this jobs are scheduled on demand.
        AuraTopology at = atb1.build("Job " + Integer.toString(1), EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING));
        ac.submitTopology(at, null);

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // lcs.shutdown();
    }
}
