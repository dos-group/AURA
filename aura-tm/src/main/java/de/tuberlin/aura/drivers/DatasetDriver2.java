package de.tuberlin.aura.drivers;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.dataflow.datasets.AbstractDataset;
import de.tuberlin.aura.core.dataflow.datasets.DatasetFactory;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.impl.ExecutionContext;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RecordReader;
import de.tuberlin.aura.core.record.RecordWriter;
import de.tuberlin.aura.core.record.tuples.Tuple4;
import de.tuberlin.aura.core.record.tuples.Tuple5;
import de.tuberlin.aura.core.taskmanager.common.TaskStates;
import de.tuberlin.aura.core.taskmanager.spi.AbstractInvokeable;
import de.tuberlin.aura.core.taskmanager.spi.IRecordReader;
import de.tuberlin.aura.core.taskmanager.spi.IRecordWriter;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;


public class DatasetDriver2 extends AbstractInvokeable {

    // ---------------------------------------------------

    public enum DatasetState {

        DATASET_EMPTY,

        DATASET_FILLED,

        DATASET_ITERATION_STATE
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Descriptors.DatasetNodeDescriptor nodeDescriptor;

    private final IExecutionContext context;

    private final AbstractDataset<Object> dataset;

    private final BlockingQueue<List<Descriptors.AbstractNodeDescriptor>> bindingRequest;

    private final Queue<Tuple5<UUID, Partitioner.PartitioningStrategy, int[][], Boolean, AbstractDataset.DatasetType>> requestProperties;

    // ---------------------------------------------------

    public DatasetState state = DatasetState.DATASET_EMPTY;

    public AbstractDataset.DatasetType type = AbstractDataset.DatasetType.UNKNOWN;

    private boolean hasInitialBinding;

    private IRecordWriter writer;

    private List<Descriptors.AbstractNodeDescriptor> currentBinding = null;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DatasetDriver2(final ITaskRuntime runtime,
                          final Descriptors.DatasetNodeDescriptor nodeDescriptor,
                          final Descriptors.NodeBindingDescriptor bindingDescriptor) {

        this.nodeDescriptor = nodeDescriptor;

        this.context = new ExecutionContext(runtime, nodeDescriptor, bindingDescriptor);

        this.dataset = (AbstractDataset<Object>)DatasetFactory.createDataset(context);

        this.bindingRequest = new LinkedBlockingQueue<>();

        this.requestProperties = new LinkedList<>();

        this.type = nodeDescriptor.datasetType;

        if (!bindingDescriptor.outputGateBindings.isEmpty()) {

            bindingRequest.addAll(bindingDescriptor.outputGateBindings);

            if (nodeDescriptor.name.equals("Dataset2")) {
                System.out.println();
            }

            requestProperties.add(
                    new Tuple5<>(
                            nodeDescriptor.topologyID,
                            nodeDescriptor.propertiesList.get(0).strategy,
                            nodeDescriptor.propertiesList.get(0).partitionKeyIndices,
                            nodeDescriptor.isReExecutable,
                            nodeDescriptor.datasetType
                    )
            );

            hasInitialBinding = true;
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void create() throws Throwable {

        consumer.openGate(0);
    }

    @Override
    public void open() throws Throwable {
    }

    @Override
    public void run() throws Throwable {

        if (state == DatasetState.DATASET_EMPTY) {

            dataset.clear();

            produceDataset(0);

            state = DatasetState.DATASET_FILLED;

        } else if (state == DatasetState.DATASET_FILLED) {

            waitForOutputBinding(0);

            consumeDataset();

        } else if (state == DatasetState.DATASET_ITERATION_STATE) {

            final CountDownLatch executeLatch = new CountDownLatch(1);

            runtime.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_RUNNING,
                    new StateMachine.IFSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                        @Override
                        public void stateAction(TaskStates.TaskState previousState,
                                                TaskStates.TaskTransition transition,
                                                TaskStates.TaskState state) {

                            executeLatch.countDown();
                        }
                    });

            try {
                executeLatch.await();
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage(), e);
            }

            consumeDataset();
        }
    }

    @Override
    public void close() throws Throwable {
    }

    @Override
    public void release() throws Throwable {

        releaseOutputBinding(0);

        //consumer.closeGate(gateIndex);
    }

    // ---------------------------------------------------

    public void createOutputBinding(final UUID topologyID,
                                    final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding,
                                    final Partitioner.PartitioningStrategy partitioningStrategy,
                                    final int[][] partitioningKeys,
                                    final boolean isReExecutable,
                                    final AbstractDataset.DatasetType datasetType) {
        // sanity check.
        if(topologyID == null)
            throw new IllegalArgumentException("topologyID == null");
        if(outputBinding == null)
            throw new IllegalArgumentException("outputBinding == null");
        if(partitioningStrategy == null)
            throw new IllegalArgumentException("partitioningStrategy == null");
        if(partitioningKeys == null)
            throw new IllegalArgumentException("partitionKeyIndices == null");

        requestProperties.add(new Tuple5<>(topologyID, partitioningStrategy, partitioningKeys, isReExecutable, datasetType));

        bindingRequest.addAll(outputBinding);
    }

    public Collection<Object> getData() {
        return dataset.getData();
    }

    // ---------------------------------------------------

    private void produceDataset(final int gateIndex) {

        final IRecordReader reader = new RecordReader(runtime, gateIndex);

        reader.begin();

        Object object = reader.readObject();

        while (object != null) {

            dataset.add(object);

            object = reader.readObject();
        }

        reader.end();

        if (runtime.getBindingDescriptor().outputGateBindings.isEmpty()) {

            runtime.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                    new StateMachine.IFSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                        @Override
                        public void stateAction(TaskStates.TaskState previousState,
                                                TaskStates.TaskTransition transition,
                                                TaskStates.TaskState state) {

                            if (!runtime.getNodeDescriptor().isReExecutable) {

                                runtime.getBindingDescriptor().inputGateBindings.clear();

                                runtime.getBindingDescriptor().outputGateBindings.clear();

                                runtime.getTaskStateMachine().reset();
                            }
                        }
                    }
            );
        }
    }

    private void waitForOutputBinding(final int gateIndex) {

        try {

            currentBinding = bindingRequest.take();

        } catch(InterruptedException e) {
            LOG.info(e.getMessage());
        }

        if (currentBinding != null) {

            final Tuple5<UUID, Partitioner.PartitioningStrategy, int[][], Boolean, AbstractDataset.DatasetType> gateRequestProperties = requestProperties.poll();

            runtime.getNodeDescriptor().topologyID = gateRequestProperties._1;

            final Partitioner.PartitioningStrategy partitioningStrategy = gateRequestProperties._2;

            final int[][] partitioningKey = gateRequestProperties._3;

            runtime.getNodeDescriptor().isReExecutable = gateRequestProperties._4;

            type = gateRequestProperties._5;

            final Partitioner.IPartitioner partitioner =
                    Partitioner.PartitionerFactory.createPartitioner(

                            partitioningStrategy,

                            nodeDescriptor.propertiesList.get(0).outputType,

                            partitioningKey
                    );

            if (!hasInitialBinding) {

                final List<List<Descriptors.AbstractNodeDescriptor>> ob = new ArrayList<>();

                ob.add(currentBinding);

                runtime.getBindingDescriptor().addOutputGateBinding(ob);

                consumer.bind(runtime.getBindingDescriptor().inputGateBindings, consumer.getAllocator());

                producer.bind(runtime.getBindingDescriptor().outputGateBindings, producer.getAllocator());

                final CountDownLatch awaitRunningTransition = new CountDownLatch(1);

                runtime.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_RUNNING,
                        new StateMachine.IFSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                            @Override
                            public void stateAction(TaskStates.TaskState previousState,
                                                    TaskStates.TaskTransition transition,
                                                    TaskStates.TaskState state) {

                                awaitRunningTransition.countDown();
                            }
                        }
                );

                try {
                    awaitRunningTransition.await();
                } catch (InterruptedException e) {
                    LOG.info(e.getMessage());
                }
            }

            writer = new RecordWriter(runtime, nodeDescriptor.propertiesList.get(0).outputType, gateIndex, partitioner);

        } else {

            throw new IllegalStateException("no output binding");
        }
    }

    private void consumeDataset() {

        writer.begin();

        for (final Object object : dataset.getData())
            writer.writeObject(object);

        writer.end();
    }

    private void releaseOutputBinding(final int gateIndex) {

        if (currentBinding != null) {
            producer.done(gateIndex);
        }

        //final CountDownLatch awaitFinishTransition = new CountDownLatch(2);

        runtime.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                new StateMachine.IFSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                    @Override
                    public void stateAction(TaskStates.TaskState previousState,
                                            TaskStates.TaskTransition transition,
                                            TaskStates.TaskState state) {

                        runtime.getBindingDescriptor().inputGateBindings.clear();
                        runtime.getBindingDescriptor().outputGateBindings.clear();
                        runtime.getTaskStateMachine().reset();

                        hasInitialBinding = false;
                    }
                });

        state = DatasetState.DATASET_FILLED;

        /*runtime.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, new IEventHandler() {

            @Override
            public void handleEvent(Event event) {
                awaitFinishTransition.countDown();
            }
        });

        try {
            awaitFinishTransition.await();
        } catch (InterruptedException e) {
            LOG.info(e.getMessage());
        }*/
    }

    public AbstractDataset<Object> getDataset() {
        return dataset;
    }
}