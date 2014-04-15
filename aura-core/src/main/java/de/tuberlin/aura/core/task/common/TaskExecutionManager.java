package de.tuberlin.aura.core.task.common;


import java.util.UUID;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.memory.MemoryManager;

public final class TaskExecutionManager extends EventDispatcher {

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

    private static final Logger LOG = Logger.getLogger(TaskExecutionManager.class);

    private final Descriptors.MachineDescriptor machineDescriptor;

    private final int numberOfCores;

    private final TaskExecutionUnit[] executionUnit;

    private final MemoryManager.BufferMemoryManager bufferMemoryManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param machineDescriptor
     * @param bufferMemoryManager
     */
    public TaskExecutionManager(final Descriptors.MachineDescriptor machineDescriptor, final MemoryManager.BufferMemoryManager bufferMemoryManager) {
        // TODO: Cleanup
        super(true, "TaskExecutionManagerEventDispatcher");

        // sanity check.
        if (machineDescriptor == null)
            throw new IllegalArgumentException("machineDescriptor == null");
        if (bufferMemoryManager == null)
            throw new IllegalArgumentException("bufferMemoryManager == null");

        this.machineDescriptor = machineDescriptor;

        this.bufferMemoryManager = bufferMemoryManager;

        this.numberOfCores = machineDescriptor.hardware.cpuCores;

        this.executionUnit = new TaskExecutionUnit[numberOfCores];

        initializeExecutionUnits();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * @param driverContext
     */
    public void scheduleTask(final TaskDriverContext driverContext) {
        // sanity check.
        if (driverContext == null)
            throw new IllegalArgumentException("driverContext == null");

        int tmpMin, tmpMinOld;
        tmpMinOld = executionUnit[0].getNumberOfEnqueuedTasks();
        int selectedEU = 0;

        for (int i = 1; i < numberOfCores; ++i) {
            tmpMin = executionUnit[i].getNumberOfEnqueuedTasks();
            if (tmpMin < tmpMinOld) {
                tmpMinOld = tmpMin;
                selectedEU = i;
            }
        }

        driverContext.setAssignedExecutionUnitIndex(selectedEU);
        executionUnit[selectedEU].enqueueTask(driverContext);

        LOG.info("EXECUTE TASK " + driverContext.taskDescriptor.name + " [" + driverContext.taskDescriptor.taskID + "]" + " ON EXECUTION UNIT ("
                + executionUnit[selectedEU].getExecutionUnitID() + ") ON MACHINE [" + machineDescriptor.uid + "]");
    }

    /**
     * @param taskID
     * @return
     */
    public TaskExecutionUnit findTaskExecutionUnitByTaskID(final UUID taskID) {
        // sanity check.
        if (taskID == null)
            throw new IllegalArgumentException("taskID == null");

        for (int i = 0; i < numberOfCores; ++i) {
            final TaskExecutionUnit eu = executionUnit[i];
            final TaskDriverContext taskCtx = eu.getCurrentTaskDriverContext();
            if (taskCtx != null && taskID.equals(taskCtx.taskDescriptor.taskID)) {
                return eu;
            }
        }

        return null;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     *
     */
    private void initializeExecutionUnits() {
        for (int i = 0; i < numberOfCores; ++i) {
            final MemoryManager.BufferAllocatorGroup inputBuffer = bufferMemoryManager.getBufferAllocatorGroup();
            final MemoryManager.BufferAllocatorGroup outputBuffer = bufferMemoryManager.getBufferAllocatorGroup();

            if (inputBuffer == outputBuffer) {
                LOG.error("SAME ALLOCATOR GROUP USED");
                throw new RuntimeException("SAME ALLOCATOR GROUP USED");
            }

            this.executionUnit[i] = new TaskExecutionUnit(this, i, inputBuffer, outputBuffer);
            this.executionUnit[i].start();
        }
    }
}
