package de.tuberlin.aura.drivers;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.dataflow.datasets.AbstractDataset;
import de.tuberlin.aura.core.dataflow.datasets.DatasetFactory;
import de.tuberlin.aura.core.dataflow.datasets.MutableDataset;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.impl.ExecutionContext;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RecordReader;
import de.tuberlin.aura.core.record.RecordWriter;
import de.tuberlin.aura.core.taskmanager.common.TaskStates;
import de.tuberlin.aura.core.taskmanager.spi.AbstractInvokeable;
import de.tuberlin.aura.core.taskmanager.spi.IRecordReader;
import de.tuberlin.aura.core.taskmanager.spi.IRecordWriter;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;
import org.apache.commons.lang3.tuple.Triple;


public class DatasetDriver extends AbstractInvokeable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Descriptors.DatasetNodeDescriptor nodeDescriptor;

    private final IExecutionContext context;

    private final AbstractDataset<Object> dataset;

    private final BlockingQueue<List<Descriptors.AbstractNodeDescriptor>> bindingRequest;

    private final Queue<Triple<UUID,Partitioner.PartitioningStrategy,int[][]>> requestProperties;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DatasetDriver(final ITaskRuntime runtime,
                         final Descriptors.DatasetNodeDescriptor nodeDescriptor,
                         final Descriptors.NodeBindingDescriptor bindingDescriptor) {

        this.nodeDescriptor = nodeDescriptor;

        this.context = new ExecutionContext(runtime, nodeDescriptor, bindingDescriptor);

        this.dataset = (AbstractDataset<Object>)DatasetFactory.createDataset(context);

        this.bindingRequest = new LinkedBlockingQueue<>();

        this.requestProperties = new LinkedList<>();

        if (!bindingDescriptor.outputGateBindings.isEmpty()) {

            bindingRequest.addAll(bindingDescriptor.outputGateBindings);

            requestProperties.add(
                    Triple.of(
                            nodeDescriptor.topologyID,
                            nodeDescriptor.propertiesList.get(0).strategy,
                            nodeDescriptor.propertiesList.get(0).partitionKeyIndices
                    )
            );
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void create() throws Throwable {
    }

    @Override
    public void open() throws Throwable {
        consumer.openGate(0);
    }

    @Override
    public void run() throws Throwable {

        // --------------------------- PRODUCE PHASE ---------------------------

        {
            //if (runtime.getBindingDescriptor().inputGateBindings.size() != 1)
            //    throw new IllegalStateException();

            final IRecordReader reader = new RecordReader(runtime, 0);

            reader.begin();

            Object object = reader.readObject();

            while (object != null) {

                dataset.add(object);

                object = reader.readObject();
            }

            reader.end();
        }

        consumer.closeGate(0);

        // --------------------------- DATASET SINK ---------------------------

        boolean initialDataflowOutputs = true;

        if (runtime.getBindingDescriptor().outputGateBindings.isEmpty()) {

            final CountDownLatch awaitFinishTransition = new CountDownLatch(1);

            runtime.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                    new StateMachine.IFSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                        @Override
                        public void stateAction(TaskStates.TaskState previousState,
                                                TaskStates.TaskTransition transition,
                                                TaskStates.TaskState state) {

                            awaitFinishTransition.countDown();
                        }
                    }
            );

            runtime.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH));

            try {
                awaitFinishTransition.await();
            } catch (InterruptedException e) {
                // do nothing.
            }

            runtime.getBindingDescriptor().inputGateBindings.clear();

            runtime.getBindingDescriptor().outputGateBindings.clear();

            runtime.getTaskStateMachine().reset();

            initialDataflowOutputs = false;
        }

        // --------------------------- CONSUME PHASE ---------------------------

        boolean isRunning = true;

        while (isRunning) {

            try {

                final List<Descriptors.AbstractNodeDescriptor> outputs = bindingRequest.take();

                final Triple<UUID,Partitioner.PartitioningStrategy,int[][]> gateRequestProperties = requestProperties.poll();

                //LOG.info("--------------> take");


                runtime.getNodeDescriptor().topologyID = gateRequestProperties.getLeft();

                final Partitioner.PartitioningStrategy partitioningStrategy = gateRequestProperties.getMiddle();

                final int[][] partitioningKey = gateRequestProperties.getRight();

                final Partitioner.IPartitioner partitioner =
                        Partitioner.PartitionerFactory.createPartitioner(

                                partitioningStrategy,

                                nodeDescriptor.propertiesList.get(0).outputType,

                                partitioningKey
                        );


                if (!initialDataflowOutputs) {

                    final List<List<Descriptors.AbstractNodeDescriptor>> ob = new ArrayList<>();

                    ob.add(outputs);

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
                    }
                }


                final IRecordWriter writer = new RecordWriter(runtime, nodeDescriptor.propertiesList.get(0).outputType, 0, partitioner);

                writer.begin();

                for (final Object object : dataset.getData())
                    writer.writeObject(object);

                writer.end();


                producer.done(0);

                final CountDownLatch awaitFinishTransition = new CountDownLatch(2);

                runtime.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                        new StateMachine.IFSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                            @Override
                            public void stateAction(TaskStates.TaskState previousState,
                                                    TaskStates.TaskTransition transition,
                                                    TaskStates.TaskState state) {
                                awaitFinishTransition.countDown();
                            }
                        });

                runtime.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, new IEventHandler() {
                    @Override
                    public void handleEvent(Event event) {
                        awaitFinishTransition.countDown();
                    }
                });

                runtime.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH));

                try {
                    awaitFinishTransition.await();
                } catch (InterruptedException e) {
                    // do nothing.
                }

                runtime.getBindingDescriptor().inputGateBindings.clear();
                runtime.getBindingDescriptor().outputGateBindings.clear();
                runtime.getTaskStateMachine().reset();

            } catch(InterruptedException e) {
                LOG.info("Dataset interrupted.");
                isRunning = false;
            }

            initialDataflowOutputs = false;
        }
    }

    @Override
    public void close() throws Throwable {
        if (!runtime.getBindingDescriptor().outputGateBindings.isEmpty()) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void release() throws Throwable {

    }

    public void createOutputBinding(final UUID topologyID,
                                    final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding,
                                    final Partitioner.PartitioningStrategy partitioningStrategy,
                                    final int[][] partitioningKeys) {
        // sanity check.
        if(topologyID == null)
            throw new IllegalArgumentException("topologyID == null");
        if(outputBinding == null)
            throw new IllegalArgumentException("outputBinding == null");
        if(partitioningStrategy == null)
            throw new IllegalArgumentException("partitioningStrategy == null");
        if(partitioningKeys == null)
            throw new IllegalArgumentException("partitionKeyIndices == null");

        requestProperties.add(Triple.of(topologyID, partitioningStrategy, partitioningKeys));

        bindingRequest.addAll(outputBinding);
    }

    public Collection<Object> getData() {
        return dataset.getData();
    }

    public AbstractDataset getDataset() {
        return dataset;
    }
}