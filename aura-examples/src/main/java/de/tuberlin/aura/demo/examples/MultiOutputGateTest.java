package de.tuberlin.aura.demo.examples;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import org.slf4j.Logger;

/**
 *
 */
public class MultiOutputGateTest {

    public static class Source extends AbstractInvokeable {

        private static final int BUFFER_COUNT = 10;

        public Source(final ITaskDriver driver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(driver, producer, consumer, LOG);
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
            producer.done();
        }
    }

    public static class ForwardLeft extends AbstractInvokeable {

        public ForwardLeft(final ITaskDriver driver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
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
                    event.buffer.free();
                    final MemoryView buffer = producer.getAllocator().allocBlocking();
                    producer.broadcast(0, buffer);
                }
            }
        }

        @Override
        public void close() throws Throwable {
            producer.done();
        }
    }


    public static class ForwardRight extends AbstractInvokeable {

        public ForwardRight(final ITaskDriver driver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
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
                    event.buffer.free();
                    final MemoryView buffer = producer.getAllocator().allocBlocking();
                    producer.broadcast(0, buffer);
                }
            }
        }

        @Override
        public void close() throws Throwable {
            producer.done();
        }
    }


    public static class Sink extends AbstractInvokeable {

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
                    event.buffer.free();
                }
            }
        }
    }

    public static void main(final String[] args) {

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));


        ac.awaitSubmissionResult(1);
        ac.closeSession();
        lcs.shutdown();
    }

}
