package de.tuberlin.aura.core.iosystem;

/**
 * IO related constants.
 */
public final class IOConfig {

    private IOConfig() {}

    /**
     * The size of the buffers hold by the
     * {@link de.tuberlin.aura.core.iosystem.IOEvents.TransferBufferEvent}.
     */
    // TODO [config]: get from buffer memory manager
    public static final int TRANSFER_BUFFER_SIZE = 64 << 10;

    /**
     * The amount of bytes that can be pushed to the netty oubound buffer before it triggers a high
     * water mark event.
     */
    public static final int NETTY_HIGH_WATER_MARK = TRANSFER_BUFFER_SIZE;

    /**
     * The amount of bytes the data in the netty outbound buffer has to fall below in order to
     * trigger a low water mark event.
     */
    public static final int NETTY_LOW_WATER_MARK = NETTY_HIGH_WATER_MARK / 2;

    /**
     * The size netty tries to set the system receive buffer to.
     */
    //public static final int NETTY_RECEIVE_BUFFER_SIZE = TRANSFER_BUFFER_SIZE;

    // TODO: replace with static event size analysis
    /**
     * The max. size a event an event sent can have.
     */
    // TODO [config]: IO.MAX_EVENT_SIZE
    public final static int MAX_EVENT_SIZE = TRANSFER_BUFFER_SIZE + 8 * 6 + // max. event meta data
            // (in transferbuffer)
            4 + // kryo class identifier
            4; // length field


    //public static final int KRYO_IO_EVENT_ID = 10;

    // TODO [config]: IO.KRYO.DATA_EVENT_ID
    public static final int KRYO_IO_DATA_EVENT_ID = 11;

    // TODO [config]: IO.KRYO.TRANSFER_EVENT_ID
    public static final int KRYO_IO_TRANSFER_EVENT_ID = 12;
}
