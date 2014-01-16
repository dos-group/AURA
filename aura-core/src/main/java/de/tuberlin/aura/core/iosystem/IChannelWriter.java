package de.tuberlin.aura.core.iosystem;

public interface IChannelWriter {

    void write(IOEvents.DataIOEvent event);

    void shutdown();
}
