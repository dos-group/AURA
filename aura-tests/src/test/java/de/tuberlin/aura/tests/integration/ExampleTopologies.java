package de.tuberlin.aura.tests.integration;

import java.util.UUID;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.taskmanager.spi.AbstractInvokeable;
import de.tuberlin.aura.core.topology.Topology;

/**
 *
 */
public class ExampleTopologies {

    // --------------------------------------------------
    // 2 layered
    // --------------------------------------------------

    public static Topology.AuraTopology two_layer_point2point_small(final AuraClient auraClient, int executionUnits) {
        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source", executionUnits / 2, 1), SmallSource.class)
           .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);

        return atb.build("Job: 2 layered - point2point connection (small)");
    }

    public static Topology.AuraTopology two_layer_point2point_large(final AuraClient auraClient, int executionUnits) {
        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source", executionUnits / 2, 1), LargeSource.class)
           .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);

        return atb.build("Job: 2 layered - point2point connection (large)");
    }

    // --------------------------------------------------
    // 3 layered
    // --------------------------------------------------

    public static Topology.AuraTopology three_layer_point2point(final AuraClient auraClient, int executionUnits) {
        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        return atb.build("Job: 3 layered - point2point connection");
    }

    public static Topology.AuraTopology three_layer_all2all_point2point(final AuraClient auraClient, int executionUnits) {
        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        return atb.build("Job: 3 layered - all2all + point2point connection");
    }

    public static Topology.AuraTopology three_layer_point2point_all2all(final AuraClient auraClient, int executionUnits) {
        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        return atb.build("Job: 3 layered - point2point + all2all connection");
    }

    public static Topology.AuraTopology three_layer_all2all_all2all(final AuraClient auraClient, int executionUnits) {
        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        return atb.build("Job: 3 layered - all2all + all2all connection");
    }

    public static Topology.AuraTopology three_layer_point2point_join_point2point(final AuraClient auraClient, int executionUnits) {
        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        return atb.build("Job: 3 layered - point2point (join) point2point connection");
    }

    public static Topology.AuraTopology three_layer_all2all_join_point2point(final AuraClient auraClient, int executionUnits) {
        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        return atb.build("Job: 3 layered - all2all (join) point2point connection");
    }

    public static Topology.AuraTopology three_layer_all2all_join_all2all(final AuraClient auraClient, int executionUnits) {
        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        return atb.build("Job: 3 layered - all2all (join) all2all connection");
    }

    public static Topology.AuraTopology three_layer_all2all_join_all2all_sl(final AuraClient auraClient, int executionUnits) {
        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), SmallSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        return atb.build("Job: 3 layered - all2all (join) all2all connection (small/large)");
    }

    // --------------------------------------------------
    // 6 layered
    // --------------------------------------------------

    public static Topology.AuraTopology six_layer_all2all(final AuraClient auraClient, int executionUnits) {
        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source Left", executionUnits / 6, 1), SmallSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source Right", executionUnits / 6, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Middle", executionUnits / 6, 1), ForwardWithTwoInputs.class)
           .connectTo("Middle2", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source Middle", executionUnits / 6, 1), SmallSource.class)
           .connectTo("Middle2", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Middle2", executionUnits / 6, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", executionUnits / 6, 1), Sink.class);

        return atb.build("Job: 6 layered - all2all connection");
    }

    // --------------------------------------------------
    // Inner Classes (Node Types)
    // --------------------------------------------------

    public static class SmallSource extends AbstractInvokeable {

        private static final int BUFFER_COUNT = 5;

        public SmallSource() {
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
            producer.done(0);
        }
    }

    public static class LargeSource extends AbstractInvokeable {

        private static final int BUFFER_COUNT = 10;

        public LargeSource() {
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
            producer.done(0);
        }
    }

    public static class ForwardWithOneInput extends AbstractInvokeable {

        public ForwardWithOneInput() {
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
            LOG.debug("{} {} done", driver.getNodeDescriptor().name, driver.getNodeDescriptor().taskIndex);
            producer.done(0);
        }
    }

    public static class ForwardWithTwoInputs extends AbstractInvokeable {

        public ForwardWithTwoInputs() {
        }

        @Override
        public void open() throws Throwable {

            consumer.openGate(0);

            consumer.openGate(1);
        }

        @Override
        public void run() throws Throwable {

            while (!consumer.isExhausted() && isInvokeableRunning()) {

                final IOEvents.TransferBufferEvent leftEvent = consumer.absorb(0);

                final IOEvents.TransferBufferEvent rightEvent = consumer.absorb(1);

                if (leftEvent != null) {

                    leftEvent.buffer.free();

                    final MemoryView buffer = producer.getAllocator().allocBlocking();

                    producer.broadcast(0, buffer);
                }

                if (rightEvent != null) {

                    rightEvent.buffer.free();

                    final MemoryView buffer = producer.getAllocator().allocBlocking();

                    producer.broadcast(0, buffer);
                }
            }
        }

        @Override
        public void close() throws Throwable {
            LOG.debug("{} {} done", driver.getNodeDescriptor().name, driver.getNodeDescriptor().taskIndex);
            producer.done(0);
        }
    }

    public static class Sink extends AbstractInvokeable {

        long count = 0;

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

                    count++;

                    event.buffer.free();
                }
            }

            LOG.info("Sink finished {}.", count);
        }
    }

}
