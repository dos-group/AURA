package de.tuberlin.aura.core.task.spi;


import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.IWM2TMProtocol;

/**
 *
 */
public interface ITaskManager extends IWM2TMProtocol {

    public abstract IOManager getIOManager();

    public abstract RPCManager getRPCManager();

    public abstract ITaskExecutionManager getTaskExecutionManager();


    public abstract Descriptors.MachineDescriptor getWorkloadManagerMachineDescriptor();

    public abstract Descriptors.MachineDescriptor getTaskManagerMachineDescriptor();
}
