package de.tuberlin.aura.taskmanager;


import java.util.UUID;

import de.tuberlin.aura.core.taskmanager.spi.*;
import de.tuberlin.aura.drivers.DatasetDriver2;
import de.tuberlin.aura.drivers.OperatorDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.memory.BufferAllocatorGroup;
import de.tuberlin.aura.core.memory.spi.IBufferMemoryManager;


public final class TaskExecutionManager extends EventDispatcher implements ITaskExecutionManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionManager.class);

    private final Descriptors.MachineDescriptor machineDescriptor;

    private final int numberOfExecutionUnits;

    private final ITaskExecutionUnit[] executionUnit;

    private final IBufferMemoryManager bufferMemoryManager;

    private final ITaskManager taskManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskExecutionManager(final ITaskManager taskManager,
                                final Descriptors.MachineDescriptor machineDescriptor,
                                final IBufferMemoryManager bufferMemoryManager,
                                final int numberOfExecutionUnits) {
        // TODO: Cleanup
        super(true, "TaskExecutionManagerEventDispatcher");

        // sanity check.
        if (taskManager == null)
            throw new IllegalArgumentException("taskManager == null");
        if (machineDescriptor == null)
            throw new IllegalArgumentException("machineDescriptor == null");
        if (bufferMemoryManager == null)
            throw new IllegalArgumentException("bufferMemoryManager == null");

        this.taskManager = taskManager;

        this.machineDescriptor = machineDescriptor;

        this.bufferMemoryManager = bufferMemoryManager;

        this.numberOfExecutionUnits = numberOfExecutionUnits;

        this.executionUnit = new TaskExecutionUnit[numberOfExecutionUnits];

        initializeExecutionUnits();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void scheduleTask(final ITaskRuntime runtime) {
        // sanity check.
        if (runtime == null)
            throw new IllegalArgumentException("runtime == null");

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

        executionUnit[selectedEU].enqueueTask(runtime);

        StringBuilder sb = new StringBuilder();
        if (executionUnit[selectedEU].getRuntime() != null && executionUnit[selectedEU].getRuntime().getInvokeable() != null) {
            if (executionUnit[selectedEU].getRuntime().getInvokeable() instanceof DatasetDriver2)
                sb.append("Dataset NAME:" + executionUnit[selectedEU].getRuntime().getNodeDescriptor().name + " TASK ID: " + executionUnit[selectedEU].getRuntime().getNodeDescriptor().taskID);
            else
                if (executionUnit[selectedEU].getRuntime().getInvokeable() instanceof OperatorDriver)
                    sb.append("Operator NAME:" + executionUnit[selectedEU].getRuntime().getNodeDescriptor().name + " TASK ID: " + executionUnit[selectedEU].getRuntime().getNodeDescriptor().taskID);
                else
                    if (executionUnit[selectedEU].getRuntime().getInvokeable() instanceof AbstractInvokeable)
                        sb.append("Invokeable");
                    else
                        sb.append("NAME:" + executionUnit[selectedEU].getRuntime().getNodeDescriptor().name + " TASK ID: " + executionUnit[selectedEU].getRuntime().getNodeDescriptor().taskID);
        } else {
            sb.append("Empty");
        }

        LOG.info("EXECUTE TASK {}-{} [{}] ON EXECUTION UNIT ({}) ON MACHINE [{}] -- QUEUE LENGTH: {} -- CURRENTLY RUNNING {}",
                 runtime.getNodeDescriptor().name,
                 runtime.getNodeDescriptor().taskIndex,
                 runtime.getNodeDescriptor().taskID,
                 executionUnit[selectedEU].getExecutionUnitID(),
                 machineDescriptor.uid,
                 executionUnit[selectedEU].getNumberOfEnqueuedTasks(),
                 sb.toString());
    }

    public ITaskExecutionUnit getExecutionUnitByTaskID(final UUID taskID) {
        // sanity check.
        if (taskID == null)
            throw new IllegalArgumentException("taskID == null");

        ITaskExecutionUnit executionUnitForTask = findExecutionUnitByTaskID(taskID);

        // FIXME: Tasks are deployed simultaneously without any synchronization yet expect to be able to connect
        // their output channels to other tasks that might still be set up. as a temporary workaround we give tasks
        // some time to set up. a better solution would be to not poll inside the receiving deserialization handler
        // but to have the receiver answer that the dst task is not available and have the sender attempt to build
        // the channel multiple times. the problem seems to occur mostly when query plans are deployed that aren't
        // trees but instead contain tasks with more than one output edge.

        for (int i = 0; executionUnitForTask == null && i < 20; i++) {
            try {
                Thread.sleep(50);
                executionUnitForTask = this.findExecutionUnitByTaskID(taskID);
            } catch (InterruptedException e) {
                LOG.info(e.getMessage());
            }
        }

        if (executionUnitForTask == null) {
            /*for (final ITaskExecutionUnit eu : executionUnit) {
                final String euTaskName = eu.getRuntime() != null ? eu.getRuntime().getNodeDescriptor().name : "";
                final UUID euTaskID = eu.getRuntime() != null ? eu.getRuntime().getNodeDescriptor().taskID : null;
                LOG.info( "execUnitId(" + eu.getExecutionUnitID() + ") - numOfEnqueuedTasks(" + eu.getNumberOfEnqueuedTasks() + ") - currentTask(" + euTaskName +  ", " + euTaskID + ")");
            }*/
            //throw new IllegalStateException("Could not find execution unit for input edge");
            return null;
        }

        return executionUnitForTask;
    }

    private ITaskExecutionUnit findExecutionUnitByTaskID(final UUID taskID) {
        for (int i = 0; i < numberOfExecutionUnits; ++i) {
            final ITaskExecutionUnit eu = executionUnit[i];
            final ITaskRuntime runtime = eu.getRuntime();
            if (runtime != null && taskID.equals(runtime.getNodeDescriptor().taskID)) {
                return eu;
            }
        }
        return null;
    }

    /*private boolean ___isTaskEnqueuedInExecutionUnit(final UUID taskID) {
        for (int i = 0; i < numberOfExecutionUnits; ++i) {
            final ITaskExecutionUnit eu = executionUnit[i];
            final ITaskRuntime runtime = eu.getRuntimeForTaskID(taskID);
            if (runtime != null) {
                return true;
            }
        }
        return false;
    }*/

    @Override
    public ITaskManager getTaskManager() {
        return taskManager;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void initializeExecutionUnits() {
        for (int i = 0; i < this.numberOfExecutionUnits; ++i) {
            final BufferAllocatorGroup inputBuffer = bufferMemoryManager.getBufferAllocatorGroup();
            final BufferAllocatorGroup outputBuffer = bufferMemoryManager.getBufferAllocatorGroup();
            this.executionUnit[i] = new TaskExecutionUnit(this, i, inputBuffer, outputBuffer);
            this.executionUnit[i].start();
        }
    }
}
