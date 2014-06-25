package de.tuberlin.aura.demo.examples;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.operators.Operators;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RowRecordReader;
import de.tuberlin.aura.core.record.RowRecordWriter;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.topology.AuraGraph;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 *
 */
public final class OperatorTest {

    // Disallow Instantiation.
    private OperatorTest() {}

    public static final class Record1 {

        public Record1() {
        }

        public int a;

        public int b;

        public int c;

        public String d;
    }

    public static final class Record2 {

        public Record2() {
        }

        public double a;
    }

    public static final class Mapper1 implements Operators.UnaryUDFFunction<Record1, Record2> {

        @Override
        public Record2 apply(Record1 in) {
            final Record2 r = new Record2();
            r.a = in.a;
            return r;
        }
    }

    /**
     *
     */
    public static class Source extends AbstractInvokeable {

        private final RowRecordWriter recordWriter;

        public Source(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(taskDriver, producer, consumer, LOG);

            final Partitioner.IPartitioner partitioner = new Partitioner.HashPartitioner(new int[] {0});

            this.recordWriter = new RowRecordWriter(driver, Record1.class, 0, partitioner);
        }

        public void open() throws Throwable {

            recordWriter.begin();
        }


        @Override
        public void run() throws Throwable {

            //if(driver.getNodeDescriptor().taskIndex == 0) {
            for(int i = 0; i < 10000; ++i) {
                final Record1 tr = new Record1();
                tr.a = i;
                tr.b = 101;
                tr.c = 102;
                tr.d = "TASK 0";
                recordWriter.writeObject(tr);
            }
            //}

        }

        @Override
        public void close() throws Throwable {

            recordWriter.end();

            producer.done();
        }
    }

    /**
     *
     */
    public static class Sink extends AbstractInvokeable {

        private final RowRecordReader recordReader;

        public Sink(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            this.recordReader = new RowRecordReader(taskDriver, 0);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            recordReader.begin();
            while (!recordReader.finished()) {
                final Record2 obj = (Record2)recordReader.readObject();
                if (obj != null) {
                    double value0 = obj.a;
                    if(driver.getNodeDescriptor().taskIndex == 0) {
                        System.out.println(driver.getNodeDescriptor().taskID + " value0:" + value0);
                    }

                }
            }
            recordReader.end();
        }

        @Override
        public void close() throws Throwable {
        }
    }


    //------------------------------------------------------------------------------------------------------

    public static final class OperatorProperties implements Serializable {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        public final UUID operatorUID;

        public final String instanceName;

        public final int localDOP;

        public final int globalDOP;

        public final int[] keys;

        public final Partitioner.PartitioningStrategy strategy;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public OperatorProperties(final UUID operatorUID,
                                  final int localDOP,
                                  final int[] keys,
                                  final Partitioner.PartitioningStrategy strategy,
                                  final int globalDOP,
                                  final String instanceName) {
            // sanity check.
            if (operatorUID == null)
                throw new IllegalArgumentException("operatorUID == null");
            if (instanceName == null)
                throw new IllegalArgumentException("instanceName == null");

            this.operatorUID = operatorUID;

            this.localDOP = localDOP;

            this.keys = keys;

            this.strategy = strategy;

            this.globalDOP = globalDOP;

            this.instanceName = instanceName;
        }

        public OperatorProperties(final int localDOP,
                                  final int[] keys,
                                  final Partitioner.PartitioningStrategy strategy,
                                  final int globalDOP,
                                  final String instanceName) {
            this(UUID.randomUUID(), localDOP, keys, strategy, globalDOP, instanceName);
        }

        public OperatorProperties(final int[] keys,
                                  final Partitioner.PartitioningStrategy strategy,
                                  final int globalDOP,
                                  final String instanceName) {
            this(UUID.randomUUID(), 1, keys, strategy, globalDOP, instanceName);
        }

    }

    //------------------------------------------------------------------------------------------------------

    public static Pair<AuraGraph.Node,List<Class<?>>> map(final Class<? extends Operators.UnaryUDFFunction<Record1,Record2>> functionType, final OperatorProperties properties) {

        final ParameterizedType parameterizedType = (ParameterizedType) functionType.getGenericInterfaces()[0];

        final Class<?> inputType = (Class<?>) parameterizedType.getActualTypeArguments()[0];

        final Class<?> outputType = (Class<?>) parameterizedType.getActualTypeArguments()[1];

        final AuraGraph.Node node = new AuraGraph.OperatorNode(
                properties.operatorUID,
                properties.instanceName,
                properties.globalDOP,
                properties.localDOP,
                Operators.OperatorType.OPERATOR_MAP,
                properties.keys,
                properties.strategy
        );

        return new Pair<>(node, Arrays.asList(functionType, inputType, outputType));
    }

    //------------------------------------------------------------------------------------------------------

    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        new ConsoleAppender(layout);

        // Local
        final String zookeeperAddress = "localhost:2181";
        new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, 4);

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraGraph.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();

        atb1.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source", 1, 1), Arrays.asList(Source.class, Record1.class))
                .connectTo("Mapper1", AuraGraph.Edge.TransferType.ALL_TO_ALL)
                .addNode(map(Mapper1.class, new OperatorProperties(new int[] {0}, Partitioner.PartitioningStrategy.HASH_PARTITIONER, 1, "Mapper1")))
                .connectTo("Sink", AuraGraph.Edge.TransferType.ALL_TO_ALL)
                .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", 1, 1), Sink.class);

        final AuraGraph.AuraTopology at1 = atb1.build("JOB 1");

        ac.submitTopology(at1, null);
    }
}

