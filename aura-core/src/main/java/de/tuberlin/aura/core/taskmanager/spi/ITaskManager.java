package de.tuberlin.aura.core.taskmanager.spi;


import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.spi.IIOManager;
import de.tuberlin.aura.core.iosystem.spi.IRPCManager;
import de.tuberlin.aura.core.protocols.ITM2WMProtocol;
import de.tuberlin.aura.core.protocols.IWM2TMProtocol;

import java.util.UUID;


public interface ITaskManager extends IWM2TMProtocol {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract IConfig getConfig();

    public abstract IIOManager getIOManager();

    public abstract IRPCManager getRPCManager();

    public abstract ITaskExecutionManager getTaskExecutionManager();

    public abstract Descriptors.MachineDescriptor getWorkloadManagerMachineDescriptor();

    public abstract Descriptors.MachineDescriptor getTaskManagerMachineDescriptor();

    public abstract void uninstallTask(final UUID taskID);

    public abstract ITM2WMProtocol getWorkloadManagerProtocol();
}
