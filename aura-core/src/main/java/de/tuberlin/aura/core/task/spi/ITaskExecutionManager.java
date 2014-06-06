package de.tuberlin.aura.core.task.spi;

import java.util.UUID;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.iosystem.IOManager;

/**
 *
 */
public interface ITaskExecutionManager extends IEventDispatcher {

    public abstract void scheduleTask(final ITaskDriver taskDriver);

    public abstract ITaskExecutionUnit findTaskExecutionUnitByTaskID(final UUID taskID);
}
