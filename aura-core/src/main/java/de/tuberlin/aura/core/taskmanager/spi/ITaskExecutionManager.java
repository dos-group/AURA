package de.tuberlin.aura.core.taskmanager.spi;

import java.util.UUID;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;


public interface ITaskExecutionManager extends IEventDispatcher {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void scheduleTask(final ITaskDriver taskDriver);

    public abstract ITaskExecutionUnit findExecutionUnitByTaskID(final UUID taskID);

    public abstract ITaskManager getTaskManager();
}
