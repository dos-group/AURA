package de.tuberlin.aura.datasets;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RowRecordReader;
import de.tuberlin.aura.core.record.RowRecordWriter;
import de.tuberlin.aura.core.taskmanager.common.TaskStates;
import de.tuberlin.aura.core.taskmanager.spi.AbstractInvokeable;
import de.tuberlin.aura.core.taskmanager.spi.IRecordReader;
import de.tuberlin.aura.core.taskmanager.spi.IRecordWriter;
import org.apache.commons.lang3.tuple.Triple;

/**
 *
 */
public class DatasetDriver extends AbstractInvokeable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Descriptors.DatasetNodeDescriptor datasetNodeDescriptor;

    private final List<Object> data;

    private final BlockingQueue<List<Descriptors.AbstractNodeDescriptor>> requestingTopologyBinding;

    private final Queue<Triple<UUID,Partitioner.PartitioningStrategy,int[][]>> requestingTopologyProperties;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DatasetDriver(final Descriptors.DatasetNodeDescriptor datasetNodeDescriptor,
                         final Descriptors.NodeBindingDescriptor bindingDescriptor) {

        this.datasetNodeDescriptor = datasetNodeDescriptor;

        this.data = new ArrayList<>();

        this.requestingTopologyBinding = new LinkedBlockingQueue<>();

        this.requestingTopologyProperties = new LinkedList<>();

        if (!bindingDescriptor.outputGateBindings.isEmpty()) {

            requestingTopologyBinding.addAll(bindingDescriptor.outputGateBindings);

            requestingTopologyProperties.add(
                    Triple.of(
                            datasetNodeDescriptor.topologyID,
                            datasetNodeDescriptor.properties.strategy,
                            datasetNodeDescriptor.properties.partitioningKeys
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
            if (driver.getBindingDescriptor().inputGateBindings.size() != 1)
                throw new IllegalStateException();

            final IRecordReader reader = new RowRecordReader(driver, 0);

            reader.begin();

            Object object = reader.readObject();

            while (object != null) {

                data.add(object);

                object = reader.readObject();
            }

            reader.end();
        }

        consumer.closeGate(0);

        // --------------------------- DATASET SINK ---------------------------

        boolean initialDataflowOutputs = true;

        if (driver.getBindingDescriptor().outputGateBindings.isEmpty()) {

            final CountDownLatch awaitFinishTransition = new CountDownLatch(1);

            driver.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                    new StateMachine.IFSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                        @Override
                        public void stateAction(TaskStates.TaskState previousState,
                                                TaskStates.TaskTransition transition,
                                                TaskStates.TaskState state) {

                            awaitFinishTransition.countDown();
                        }
                    }
            );

            driver.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH));

            try {
                awaitFinishTransition.await();
            } catch (InterruptedException e) {
                // do nothing.
            }

            driver.getBindingDescriptor().inputGateBindings.clear();

            driver.getBindingDescriptor().outputGateBindings.clear();

            driver.getTaskStateMachine().reset();

            initialDataflowOutputs = false;
        }

        // --------------------------- CONSUME PHASE ---------------------------

        while (true) {

            try {

                final List<Descriptors.AbstractNodeDescriptor> outputs = requestingTopologyBinding.take();

                final Triple<UUID,Partitioner.PartitioningStrategy,int[][]> gateRequestProperties = requestingTopologyProperties.poll();

                driver.getNodeDescriptor().topologyID = gateRequestProperties.getLeft();

                final Partitioner.PartitioningStrategy partitioningStrategy = gateRequestProperties.getMiddle();

                final int[][] partitioningKey = gateRequestProperties.getRight();

                final Partitioner.IPartitioner partitioner =
                        Partitioner.PartitionerFactory.createPartitioner(

                                partitioningStrategy,

                                datasetNodeDescriptor.properties.outputType,

                                partitioningKey
                        );


                if (!initialDataflowOutputs) {

                    List<List<Descriptors.AbstractNodeDescriptor>> ob = new ArrayList<>();

                    ob.add(outputs);

                    driver.getBindingDescriptor().addOutputGateBinding(ob);

                    consumer.bind(driver.getBindingDescriptor().inputGateBindings, consumer.getAllocator());

                    producer.bind(driver.getBindingDescriptor().outputGateBindings, producer.getAllocator());

                    final CountDownLatch awaitRunningTransition = new CountDownLatch(1);

                    driver.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_RUNNING,
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
                        // do nothing.
                    }
                }



                final IRecordWriter writer = new RowRecordWriter(driver, datasetNodeDescriptor.properties.outputType, 0, partitioner);

                writer.begin();

                for (final Object object : data) {
                    writer.writeObject(object);
                }

                writer.end();

                final CountDownLatch awaitFinishTransition = new CountDownLatch(2);

                driver.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                        new StateMachine.IFSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                            @Override
                            public void stateAction(TaskStates.TaskState previousState,
                                                    TaskStates.TaskTransition transition,
                                                    TaskStates.TaskState state) {
                                awaitFinishTransition.countDown();
                        }
                        });

                driver.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, new IEventHandler() {
                    @Override
                    public void handleEvent(Event event) {
                        awaitFinishTransition.countDown();
                        }
                });

                driver.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH));

                try {
                    awaitFinishTransition.await();
                } catch (InterruptedException e) {
                    // do nothing.
                }


                driver.getBindingDescriptor().inputGateBindings.clear();

                driver.getBindingDescriptor().outputGateBindings.clear();

                driver.getTaskStateMachine().reset();

            } catch(Exception e) {
                // do nothing.
            }

            initialDataflowOutputs = false;
        }
    }

    @Override
    public void close() throws Throwable {

        //if (driver.getBindingDescriptor().outputGateBindings.size() == 1)
        //    producer.done(0);

        if (!driver.getBindingDescriptor().outputGateBindings.isEmpty()) {
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
            throw new IllegalArgumentException("partitioningKeys == null");

        requestingTopologyBinding.addAll(outputBinding);

        requestingTopologyProperties.add(Triple.of(topologyID, partitioningStrategy, partitioningKeys));
    }
}
