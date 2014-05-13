package de.tuberlin.aura.core.iosystem.spi;

import de.tuberlin.aura.core.descriptors.Descriptors;

/**
 *
 */
public interface IRPCManager {

    public abstract void registerRPCProtocolImpl(final Object protocolImplementation, final Class<?> protocolInterface);

    public abstract <T> T getRPCProtocolProxy(final Class<T> protocolInterface, final Descriptors.MachineDescriptor dstMachine);
}
