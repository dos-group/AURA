package de.tuberlin.aura.tests.clients;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;

public class LocalExecutionTest {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(LocalExecutionTest.class);

    private static final String zookeeperAddress = "localhost:2181";
    private static final int machines = 2;

    private static LocalClusterSimulator lce;
    private static AuraClient ac;

    // ---------------------------------------------------
    // Tests.
    // ---------------------------------------------------

    @BeforeClass
    public static void setupClusterSimulatorAndClient() {
        lce = new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
                        true,
                        zookeeperAddress,
                        machines);

        ac = new AuraClient(zookeeperAddress, 10000, 11111);
    }

    @Test
    public void testMinimalTopology() {
        List<Topology.AuraTopology> topologies = Collections.singletonList(minimalTestTopology(ac, machines, 2));
        SubmissionHandler handler = new SubmissionHandler(ac, topologies, 1);

        ac.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, handler);

        handler.handleTopologyFinished(null);

        ac.awaitSubmissionResult();
    }

    @AfterClass
    public static void tearDown() {
        ac.closeSession();
    }

    // ---------------------------------------------------
    // Methods.
    // ---------------------------------------------------

    private static Topology.AuraTopology minimalTestTopology(AuraClient client, int machines, int tasksPerMachine) {
        int executionUnits = machines * tasksPerMachine;
        Topology.AuraTopologyBuilder atb;

        atb = client.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 2, 1), Source.class)
                .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);

        return atb.build("Job: 2 layered - point2point connection");
    }


    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static class Source extends AbstractInvokeable {

        private static final int BUFFER_COUNT = 5;

        public Source(final ITaskDriver driver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(driver, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {

            int i = 0;
            while (i++ < BUFFER_COUNT && isInvokeableRunning()) {

                final MemoryView buffer = producer.getAllocator().allocBlocking();

                producer.broadcast(0, buffer);
            }

            LOG.info("Source finished");
        }

        @Override
        public void close() throws Throwable {
            LOG.debug("{} {} done", driver.getNodeDescriptor().name, driver.getNodeDescriptor().taskIndex);
            producer.done();
        }
    }

    public static class Sink extends AbstractInvokeable {

        long count = 0;

        public Sink(final ITaskDriver driver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(driver, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            while (!consumer.isExhausted() && isInvokeableRunning()) {

                final IOEvents.TransferBufferEvent event = consumer.absorb(0);

                if (event != null) {

                    count++;

                    event.buffer.free();
                }
            }

            LOG.info("Sink finished {}.", count);
        }
    }

    private static class SubmissionHandler extends EventHandler {

        private final AuraClient client;

        private final List<Topology.AuraTopology> topologies;

        private final int runs;

        private int jobCounter = 0;

        public SubmissionHandler(final AuraClient client, final List<Topology.AuraTopology> topologies, final int runs) {
            this.client = client;
            this.topologies = topologies;
            this.runs = runs;
        }

        @EventHandler.Handle(event = IOEvents.ControlIOEvent.class, type = IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED)
        private void handleTopologyFinished(final IOEvents.ControlIOEvent event) {
            if (event != null) {
                String jobName = (String) event.getPayload();
                LOG.info("Topology ({}) finished.", jobName);
            }

            if (jobCounter < runs * topologies.size()) {
                final int jobIndex = jobCounter++ / runs;

                Thread t = new Thread() {

                    public void run() {
                        // This break is only necessary to make it easier to distinguish jobs in
                        // the log files.
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        LOG.error("Submit: {} - run {}/{}", topologies.get(jobIndex).name, ((jobCounter - 1) % runs) + 1, runs);
                        client.submitTopology(topologies.get(jobIndex), null);
                    }
                };

                t.start();
            }
        }
    }


}