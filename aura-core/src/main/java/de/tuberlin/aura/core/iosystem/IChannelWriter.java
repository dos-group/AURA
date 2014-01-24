package de.tuberlin.aura.core.iosystem;

public interface IChannelWriter {

    void connect();

    void write(IOEvents.DataIOEvent event);

    void setOutputQueue(BufferQueue<IOEvents.DataIOEvent> queue);

    void shutdown();
}
