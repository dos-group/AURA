package de.tuberlin.aura.core.iosystem.spi;


import java.util.UUID;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.spi.IAllocator;
import io.netty.channel.Channel;

/**
 *
 */
public interface IIOManager {

    public abstract void connectDataChannel(final Descriptors.MachineDescriptor dstMachine, final UUID srcTaskID, final UUID dstTaskID, final IAllocator allocator);

    public abstract Channel getDataChannel(final UUID srcTaskID, final UUID dstTaskID);

    public abstract void disconnectDataChannel(final UUID srcTaskID, final UUID dstTaskID);


    public abstract void connectControlChannel(final Descriptors.MachineDescriptor dstMachine);

    public abstract Channel getControlChannel(final Descriptors.MachineDescriptor dstMachine);

    public abstract void disconnectControlChannel(final Descriptors.MachineDescriptor dstMachine);

    public abstract void sendControlEvent(final Descriptors.MachineDescriptor dstMachine, final IOEvents.ControlIOEvent event);

    public abstract void sendControlEvent(final UUID dstMachineID, final IOEvents.ControlIOEvent event);

}
