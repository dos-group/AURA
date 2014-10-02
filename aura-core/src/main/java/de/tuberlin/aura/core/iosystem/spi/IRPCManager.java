package de.tuberlin.aura.core.iosystem.spi;

import de.tuberlin.aura.core.descriptors.Descriptors;


public interface IRPCManager {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void registerRPCProtocol(final Object protocolImplementation, final Class<?> protocolInterface);

    public abstract <T> T getRPCProtocolProxy(final Class<T> protocolInterface, final Descriptors.MachineDescriptor dstMachine);
}