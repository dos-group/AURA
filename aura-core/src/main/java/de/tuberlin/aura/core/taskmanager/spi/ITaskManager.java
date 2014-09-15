package de.tuberlin.aura.core.taskmanager.spi;


import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.spi.IIOManager;
import de.tuberlin.aura.core.iosystem.spi.IRPCManager;
import de.tuberlin.aura.core.protocols.IWM2TMProtocol;

/**
 *
 */
public interface ITaskManager extends IWM2TMProtocol {

    public abstract IIOManager getIOManager();

    public abstract IRPCManager getRPCManager();

    public abstract ITaskExecutionManager getTaskExecutionManager();

    public abstract Descriptors.MachineDescriptor getWorkloadManagerMachineDescriptor();

    public abstract Descriptors.MachineDescriptor getTaskManagerMachineDescriptor();
}
