package de.tuberlin.aura.workloadmanager.spi;

import de.tuberlin.aura.core.iosystem.spi.IIOManager;
import de.tuberlin.aura.core.iosystem.spi.IRPCManager;
import org.apache.hadoop.mapred.InputSplit;

import java.util.UUID;

public interface IWorkloadManager {

    public abstract IIOManager getIOManager();

    public abstract IRPCManager getRPCManager();

    public abstract IInfrastructureManager getInfrastructureManager();

    public abstract IDistributedEnvironment getEnvironmentManager();
}
