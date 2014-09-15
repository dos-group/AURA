package de.tuberlin.aura.core.iosystem.spi;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import io.netty.channel.Channel;

import java.util.UUID;

/**
 *
 */
public interface IIOManager extends IEventDispatcher {

    public abstract void connectDataChannel(final UUID srcTaskID, final UUID dstTaskID, final Descriptors.MachineDescriptor dstMachine);

    public abstract void disconnectDataChannel(final UUID srcTaskID, final UUID dstTaskID, final Descriptors.MachineDescriptor dstMachine);

    public abstract void connectMessageChannelBlocking(final Descriptors.MachineDescriptor dstMachine);

    public abstract Channel getControlIOChannel(final Descriptors.MachineDescriptor dstMachine);

    public abstract void sendEvent(final Descriptors.MachineDescriptor dstMachine, final IOEvents.ControlIOEvent event);

    public abstract void sendEvent(final UUID dstMachineID, final IOEvents.ControlIOEvent event);
}
