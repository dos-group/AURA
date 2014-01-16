package de.tuberlin.aura.core.iosystem;

public interface IChannelReader {

    BufferQueue<IOEvents.DataIOEvent> getInputQueue(int gateIndex);

    void setInputQueue(int gateIndex, BufferQueue<IOEvents.DataIOEvent> queue);
}
