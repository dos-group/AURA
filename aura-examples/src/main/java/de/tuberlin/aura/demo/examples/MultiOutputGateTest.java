package de.tuberlin.aura.demo.examples;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.topology.Topology;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 *
 */
public class MultiOutputGateTest {

    private static final Logger LOG = LoggerFactory.getLogger(MultiOutputGateTest.class);

    // -----------------------------------------------------------------------------

    public static class Source extends AbstractInvokeable {

        private static final int BUFFER_COUNT = 10;

        public Source() {
        }

        @Override
        public void open() throws Throwable {

            driver.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, new IEventHandler() {

                @Override
                public void handleEvent(Event event) {

                }
            });
        }

        @Override
        public void run() throws Throwable {
            int i = 0;
            while (i++ < BUFFER_COUNT && isInvokeableRunning()) {
                final MemoryView buffer = producer.getAllocator().allocBlocking();
                producer.broadcast(0, buffer);
            }
        }

        @Override
        public void close() throws Throwable {
            producer.done(0);
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
        }

        @Override
        public void run() throws Throwable {
            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent event = consumer.absorb(0);
                if (event != null) {
                    event.buffer.free();
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

        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", 1, 1), Source.class)
           .connectTo("ForwardLeft", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "ForwardLeft", 1, 1), ForwardLeft.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           //.addNode(new Topology.ComputationNode(UUID.randomUUID(), "ForwardRight", 1, 1), ForwardRight.class)
           //.connectTo("Middle2", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", 1, 1), Sink.class);

        ac.submitTopology(atb.build("JOB1"), null);

        ac.awaitSubmissionResult(1);
        ac.closeSession();
        lcs.shutdown();
    }
}
