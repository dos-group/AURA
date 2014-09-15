package de.tuberlin.aura.demo.examples;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.taskmanager.spi.AbstractInvokeable;
import de.tuberlin.aura.core.topology.Topology;

/**
 *
 */
public class MultiOutputGateTest {

    private static final Logger LOG = LoggerFactory.getLogger(MultiOutputGateTest.class);

    // -----------------------------------------------------------------------------

    public static class Source extends AbstractInvokeable {

        private static final int BUFFER_COUNT = 10;

        private BlockingQueue<Integer> gateIndexQueue = new LinkedBlockingQueue<>();

        private Set<Integer> openOutputGates = new HashSet<>();

        public Source() {
        }

        @Override
        public void open() throws Throwable {

            driver.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, new IEventHandler() {

                @Override
                public void handleEvent(Event event) {
                    final IOEvents.DataIOEvent gateEvent = (IOEvents.DataIOEvent)event;
                    gateIndexQueue.add(producer.getOutputGateIndexFromTaskID(gateEvent.dstTaskID));
                }
            });

            for(int i = 0; i < driver.getBindingDescriptor().outputGateBindings.size(); ++i) {
                openOutputGates.add(i);
            }
        }

        @Override
        public void run() throws Throwable {

            while(!openOutputGates.isEmpty()) {

                final int gateIndex;

                try {
                    gateIndex = gateIndexQueue.take();

                    int i = 0;
                    while (i++ < BUFFER_COUNT && isInvokeableRunning()) {
                        final MemoryView buffer = producer.getAllocator().allocBlocking();
                        producer.broadcast(gateIndex, buffer);
                    }

                    producer.done(gateIndex);
                    openOutputGates.remove(gateIndex);

                } catch(InterruptedException e) {
                    // do nothing.
                }
            }
        }

        @Override
        public void close() throws Throwable {
        }
    }

    // -----------------------------------------------------------------------------

    public static class ForwardLeft extends AbstractInvokeable {

        public ForwardLeft() {
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
                    event.buffer.free();
                    final MemoryView buffer = producer.getAllocator().allocBlocking();
                    producer.broadcast(0, buffer);
                }
            }
        }

        @Override
        public void close() throws Throwable {
            producer.done(0);
        }
    }

    // -----------------------------------------------------------------------------

    public static class ForwardRight extends AbstractInvokeable {

        public ForwardRight() {
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
                    event.buffer.free();
                    final MemoryView buffer = producer.getAllocator().allocBlocking();
                    producer.broadcast(0, buffer);
                }
            }
        }

        @Override
        public void close() throws Throwable {
            producer.done(0);
        }
    }

    // -----------------------------------------------------------------------------

    public static class Sink extends AbstractInvokeable {

        public Sink() {
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
            consumer.openGate(1);
        }

        @Override
        public void run() throws Throwable {
            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent event1 = consumer.absorb(0);
                if (event1 != null) {
                    LOG.info("-> EVENT 1");
                    event1.buffer.free();
                }
                final IOEvents.TransferBufferEvent event2 = consumer.absorb(1);
                if (event2 != null) {
                    LOG.info("-> EVENT 2");
                    event2.buffer.free();
                }
            }
        }
    }

    // -----------------------------------------------------------------------------

    public static void main(final String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        new ConsoleAppender(layout);

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));
        Topology.AuraTopologyBuilder atb = ac.createTopologyBuilder();

        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source", 1, 1), Source.class)
           .connectTo("ForwardLeft", Topology.Edge.TransferType.ALL_TO_ALL)
           .and().connectTo("ForwardRight", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "ForwardLeft", 1, 1), ForwardLeft.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "ForwardRight", 1, 1), ForwardRight.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", 1, 1), Sink.class);

        ac.submitTopology(atb.build("JOB1"), null);

        ac.awaitSubmissionResult(1);
        ac.closeSession();
        lcs.shutdown();
    }
}
