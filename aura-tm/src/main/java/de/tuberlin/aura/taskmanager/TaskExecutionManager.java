package de.tuberlin.aura.taskmanager;


import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.memory.BufferAllocatorGroup;
import de.tuberlin.aura.core.memory.spi.IBufferMemoryManager;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.task.spi.ITaskExecutionManager;
import de.tuberlin.aura.core.task.spi.ITaskExecutionUnit;

public final class TaskExecutionManager extends EventDispatcher implements ITaskExecutionManager {

    // ---------------------------------------------------
    // Execution Manager Events.
    // ---------------------------------------------------

    /**
     *
     */
    public static final class TaskExecutionEvent extends Event {

        public static final String EXECUTION_MANAGER_EVENT_UNREGISTER_TASK = "EXECUTION_MANAGER_EVENT_UNREGISTER_TASK";

        public TaskExecutionEvent(String type, Object payload) {
            super(type, payload);
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionManager.class);

    private final Descriptors.MachineDescriptor machineDescriptor;

    private final int numberOfExecutionUnits;

    private final ITaskExecutionUnit[] executionUnit;

    private final IBufferMemoryManager bufferMemoryManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param machineDescriptor
     * @param bufferMemoryManager
     */
    public TaskExecutionManager(final Descriptors.MachineDescriptor machineDescriptor,
                                final IBufferMemoryManager bufferMemoryManager,
                                final int numberOfExecutionUnits) {
        // TODO: Cleanup
        super(true, "TaskExecutionManagerEventDispatcher");

        // sanity check.
        if (machineDescriptor == null)
            throw new IllegalArgumentException("machineDescriptor == null");
        if (bufferMemoryManager == null)
            throw new IllegalArgumentException("bufferMemoryManager == null");

        this.machineDescriptor = machineDescriptor;

        this.bufferMemoryManager = bufferMemoryManager;

        this.numberOfExecutionUnits = numberOfExecutionUnits;

        this.executionUnit = new TaskExecutionUnit[numberOfExecutionUnits];

        initializeExecutionUnits();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * @param driver
     */
    public void scheduleTask(final ITaskDriver driver) {
        // sanity check.
        if (driver == null)
            throw new IllegalArgumentException("driver == null");

        int tmpMin, tmpMinOld;
        tmpMinOld = executionUnit[0].getNumberOfEnqueuedTasks();
        int selectedEU = 0;

        for (int i = 1; i < numberOfExecutionUnits; ++i) {
            tmpMin = executionUnit[i].getNumberOfEnqueuedTasks();
            if (tmpMin < tmpMinOld) {
                tmpMinOld = tmpMin;
                selectedEU = i;
            }
        }

        // driver.setAssignedExecutionUnitIndex(selectedEU);
        executionUnit[selectedEU].enqueueTask(driver);

        LOG.info("EXECUTE TASK {}-{} [{}] ON EXECUTION UNIT ({}) ON MACHINE [{}]",
                 driver.getNodeDescriptor().name,
                 driver.getNodeDescriptor().taskIndex,
                 driver.getNodeDescriptor().taskID,
                 executionUnit[selectedEU].getExecutionUnitID(),
                 machineDescriptor.uid);
    }

    /**
     * @param taskID
     * @return
     */
    public ITaskExecutionUnit findExecutionUnitByTaskID(final UUID taskID) {
        // sanity check.
        if (taskID == null)
            throw new IllegalArgumentException("taskID == null");

        for (int i = 0; i < numberOfExecutionUnits; ++i) {
            final ITaskExecutionUnit eu = executionUnit[i];
            final ITaskDriver taskCtx = eu.getCurrentTaskDriver();
            if (taskCtx != null && taskID.equals(taskCtx.getNodeDescriptor().taskID)) {
                return eu;
            }
        }

        LOG.trace("No task execution unit was found for this task ID: {}", taskID);
        return null;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     *
     */
    private void initializeExecutionUnits() {
        int numberOfExecutionUnits = this.numberOfExecutionUnits;

        for (int i = 0; i < numberOfExecutionUnits; ++i) {
            final BufferAllocatorGroup inputBuffer = bufferMemoryManager.getBufferAllocatorGroup();
            final BufferAllocatorGroup outputBuffer = bufferMemoryManager.getBufferAllocatorGroup();

            this.executionUnit[i] = new TaskExecutionUnit(this, i, inputBuffer, outputBuffer);
            this.executionUnit[i].start();
        }
    }
}
