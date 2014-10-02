package de.tuberlin.aura.core.taskmanager.spi;

import org.slf4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.QueueManager;
import de.tuberlin.aura.core.memory.spi.IAllocator;


public interface ITaskDriver extends IEventDispatcher, ITaskDriverLifecycle {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract Descriptors.AbstractNodeDescriptor getNodeDescriptor();

    public abstract Descriptors.NodeBindingDescriptor getBindingDescriptor();

    public abstract QueueManager<IOEvents.DataIOEvent> getQueueManager();

    public abstract StateMachine.FiniteStateMachine getTaskStateMachine();

    public abstract IDataProducer getDataProducer();

    public abstract IDataConsumer getDataConsumer();

    public abstract ITaskManager getTaskManager();

    public abstract Logger getLogger();

    public abstract AbstractInvokeable getInvokeable();

    public abstract void connectDataChannel(final Descriptors.AbstractNodeDescriptor dstNodeDescriptor, final IAllocator allocator);
}
