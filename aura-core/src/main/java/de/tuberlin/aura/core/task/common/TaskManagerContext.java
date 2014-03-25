package de.tuberlin.aura.core.task.common;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;

public final class TaskManagerContext {

    public final IOManager ioManager;

    public final RPCManager rpcManager;

    public final TaskExecutionManager executionManager;


    public final Descriptors.MachineDescriptor workloadManagerMachine;

    public final Descriptors.MachineDescriptor ownMachine;


    public TaskManagerContext(final IOManager ioManager,
                              final RPCManager rpcManager,
                              final TaskExecutionManager executionManager,
                              final Descriptors.MachineDescriptor workloadManagerMachine,
                              final Descriptors.MachineDescriptor ownMachine) {

        this.ioManager = ioManager;

        this.rpcManager = rpcManager;

        this.executionManager = executionManager;

        this.workloadManagerMachine = workloadManagerMachine;

        this.ownMachine = ownMachine;
    }
}
