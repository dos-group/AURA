package de.tuberlin.aura.workloadmanager.spi;

import de.tuberlin.aura.core.descriptors.Descriptors;

public interface IInfrastructureManager {

    public abstract int getNumberOfMachine();

    public abstract Descriptors.MachineDescriptor getNextMachine();
}
