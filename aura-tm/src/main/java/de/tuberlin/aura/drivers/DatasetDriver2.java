package de.tuberlin.aura.drivers;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.dataflow.datasets.AbstractDataset;
import de.tuberlin.aura.core.dataflow.datasets.DatasetFactory;
import de.tuberlin.aura.core.dataflow.datasets.ImmutableDataset;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.impl.ExecutionContext;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RecordReader;
import de.tuberlin.aura.core.record.RecordWriter;
import de.tuberlin.aura.core.taskmanager.common.TaskStates;
import de.tuberlin.aura.core.taskmanager.spi.AbstractInvokeable;
import de.tuberlin.aura.core.taskmanager.spi.IRecordReader;
import de.tuberlin.aura.core.taskmanager.spi.IRecordWriter;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;


public class DatasetDriver2 extends AbstractInvokeable {

    // ---------------------------------------------------

    private static final class OutputGateBindingProperties {

        public OutputGateBindingProperties(final UUID topologyID,
                                           final Partitioner.PartitioningStrategy partitioningStrategy,
                                           final int[][] partitioningKeys,
                                           final boolean isReExecutable,
                                           final AbstractDataset.DatasetType datasetType) {

            this.topologyID = topologyID;

            this.partitioningStrategy = partitioningStrategy;

            this.partitioningKeys = partitioningKeys;

            this.isReExecutable = isReExecutable;

            this.datasetType = datasetType;
        }

        public final UUID topologyID;

        public final Partitioner.PartitioningStrategy partitioningStrategy;

        public final int[][] partitioningKeys;

        public final boolean isReExecutable;

        public final AbstractDataset.DatasetType datasetType;
    }

    // ---------------------------------------------------

    public enum DatasetInternalState {

        DATASET_INTERNAL_STATE_EMPTY,

        DATASET_INTERNAL_STATE_FILLED,

        DATASET_INTERNAL_STATE_ITERATION
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Descriptors.DatasetNodeDescriptor nodeDescriptor;

    private final AbstractDataset<Object> dataset;

    private final BlockingQueue<Pair<List<Descriptors.AbstractNodeDescriptor>,OutputGateBindingProperties>> bindingRequest;

    // ---------------------------------------------------

    public DatasetInternalState internalState = DatasetInternalState.DATASET_INTERNAL_STATE_EMPTY;

    public AbstractDataset.DatasetType datasetType = AbstractDataset.DatasetType.UNKNOWN;

    private boolean hasInitialBinding;

    private IRecordWriter writer;

    private Pair<List<Descriptors.AbstractNodeDescriptor>,OutputGateBindingProperties> currentRequest = null;

    // ---------------------------------------------------

    private CountDownLatch awaitRunningTransition = new CountDownLatch(1);

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public DatasetDriver2(final ITaskRuntime runtime,
                          final Descriptors.DatasetNodeDescriptor nodeDescriptor,
                          final Descriptors.NodeBindingDescriptor bindingDescriptor) {

        this.nodeDescriptor = nodeDescriptor;

        final IExecutionContext context = new ExecutionContext(runtime, nodeDescriptor, bindingDescriptor);

        this.dataset = (AbstractDataset<Object>)DatasetFactory.createDataset(context);

        this.bindingRequest = new LinkedBlockingQueue<>();

        this.datasetType = nodeDescriptor.datasetType;

        if (!bindingDescriptor.outputGateBindings.isEmpty()) {

            synchronized (this) {

                final OutputGateBindingProperties requestProperties = new OutputGateBindingProperties(
                        nodeDescriptor.topologyID,
                        nodeDescriptor.propertiesList.get(0).strategy,
                        nodeDescriptor.propertiesList.get(0).partitionKeyIndices,
                        nodeDescriptor.isReExecutable,
                        nodeDescriptor.datasetType
                );

                final Pair<List<Descriptors.AbstractNodeDescriptor>, OutputGateBindingProperties> request =
                        new Pair<>(bindingDescriptor.outputGateBindings.get(0), requestProperties);

                bindingRequest.add(request);

                hasInitialBinding = true;
            }
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public void create() throws Throwable {

        runtime.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_RUNNING,
                new StateMachine.IFSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                    @Override
                    public void stateAction(TaskStates.TaskState previousState,
                                            TaskStates.TaskTransition transition,
                                            TaskStates.TaskState state) {

                        awaitRunningTransition.countDown();
                    }
                });

        runtime.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                new StateMachine.IFSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                    @Override
                    public void stateAction(TaskStates.TaskState previousState,
                                            TaskStates.TaskTransition transition,
                                            TaskStates.TaskState state) {

                        runtime.getBindingDescriptor().inputGateBindings.clear();

                        runtime.getBindingDescriptor().outputGateBindings.clear();

                        hasInitialBinding = false;
                    }
                }
        );

        consumer.openGate(0);
    }

    @Override
    public void open() throws Throwable {
    }

    @Override
    public void run() throws Throwable {

        if (internalState == DatasetInternalState.DATASET_INTERNAL_STATE_EMPTY) {

            dataset.clear();

            produceDataset(0);

            internalState = DatasetInternalState.DATASET_INTERNAL_STATE_FILLED;

        } else if (internalState == DatasetInternalState.DATASET_INTERNAL_STATE_FILLED) {

            if (waitForOutputBinding(0) != null) {
                consumeDataset();
            }

        } else if (internalState == DatasetInternalState.DATASET_INTERNAL_STATE_ITERATION) {

            try {
                awaitRunningTransition.await();
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage(), e);
            }

            awaitRunningTransition = new CountDownLatch(1);

            consumeDataset();
        }
    }

    @Override
    public void close() throws Throwable {
    }

    @Override
    public void release() throws Throwable {

        if (currentRequest != null) {
            producer.done(0);
        }

        internalState = DatasetInternalState.DATASET_INTERNAL_STATE_FILLED;

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
        if(datasetType == null)
            throw new IllegalArgumentException("datasetType == null");

        synchronized (this) {

            final OutputGateBindingProperties requestProperties = new OutputGateBindingProperties(
                    topologyID,
                    partitioningStrategy,
                    partitioningKeys,
                    isReExecutable,
                    datasetType
            );

            final Pair<List<Descriptors.AbstractNodeDescriptor>,OutputGateBindingProperties> request =
                    new Pair<>(outputBinding.get(0), requestProperties);

            bindingRequest.add(request);
        }
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
    }

    private List<Descriptors.AbstractNodeDescriptor> waitForOutputBinding(final int gateIndex) {

        currentRequest = null;

        try {

            currentRequest = bindingRequest.take();

        } catch(InterruptedException e) {
            LOG.info(e.getMessage());
        }

        if (currentRequest != null) {

            runtime.getTaskStateMachine().reset();

            final OutputGateBindingProperties requestProperties = currentRequest.getSecond();

            runtime.getNodeDescriptor().topologyID = requestProperties.topologyID;

            final Partitioner.PartitioningStrategy partitioningStrategy = requestProperties.partitioningStrategy;

            final int[][] partitioningKey = requestProperties.partitioningKeys;

            runtime.getNodeDescriptor().isReExecutable = requestProperties.isReExecutable;

            datasetType = requestProperties.datasetType;

            final Partitioner.IPartitioner partitioner =
                    Partitioner.PartitionerFactory.createPartitioner(

                            partitioningStrategy,

                            nodeDescriptor.propertiesList.get(0).outputType,

                            partitioningKey
                    );

            if (!hasInitialBinding) {

                final List<List<Descriptors.AbstractNodeDescriptor>> ob = new ArrayList<>();

                ob.add(currentRequest.getFirst());

                runtime.getBindingDescriptor().addOutputGateBinding(ob);

                consumer.bind(runtime.getBindingDescriptor().inputGateBindings, consumer.getAllocator());

                producer.bind(runtime.getBindingDescriptor().outputGateBindings, producer.getAllocator());

                try {
                    awaitRunningTransition.await();
                } catch (InterruptedException e) {
                    LOG.info(e.getMessage());
                }

                awaitRunningTransition = new CountDownLatch(1);
            }

            writer = new RecordWriter(runtime, nodeDescriptor.propertiesList.get(0).outputType, gateIndex, partitioner);

            return currentRequest.getFirst();
        } else
            return null;
    }

    private void consumeDataset() {
        writer.begin();

        if (dataset instanceof ImmutableDataset &&
                ((ImmutableDataset) dataset).hasFixedNumberOfReads() &&
                ((ImmutableDataset) dataset).isLastRead()) {

            Queue dataQueue = (Queue) dataset.getData();

            while (dataQueue.size() > 0) {
                writer.writeObject(dataQueue.remove());
            }

        } else {
            for (final Object object : dataset.getData())
                writer.writeObject(object);
        }

        writer.end();
    }

    public AbstractDataset<Object> getDataset() {
        return dataset;
    }
}