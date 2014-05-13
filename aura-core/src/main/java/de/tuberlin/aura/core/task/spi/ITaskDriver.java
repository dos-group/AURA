package de.tuberlin.aura.core.task.spi;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.QueueManager;
import de.tuberlin.aura.core.measurement.MeasurementManager;
import de.tuberlin.aura.core.measurement.record.RecordReader;
import de.tuberlin.aura.core.measurement.record.RecordWriter;
import de.tuberlin.aura.core.memory.spi.IAllocator;

/**
 *
 */
public interface ITaskDriver extends IEventDispatcher, ITaskDriverLifecycle {

    public abstract Descriptors.TaskDescriptor getTaskDescriptor();

    public abstract Descriptors.TaskBindingDescriptor getTaskBindingDescriptor();

    public abstract QueueManager<IOEvents.DataIOEvent> getQueueManager();

    public abstract StateMachine.FiniteStateMachine getTaskStateMachine();

    public abstract void connectDataChannel(final Descriptors.TaskDescriptor dstTaskDescriptor, final IAllocator allocator);

    public abstract IDataProducer getDataProducer();

    public abstract IDataConsumer getDataConsumer();

    public abstract MeasurementManager getMeasurementManager();

    public abstract RecordReader getRecordReader();

    public abstract RecordWriter getRecordWriter();

    public abstract ITaskManager getTaskManager();
}
