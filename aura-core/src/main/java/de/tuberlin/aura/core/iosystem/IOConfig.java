package de.tuberlin.aura.core.iosystem;

public final class IOConfig {

    public static final int TRANSFER_BUFFER_SIZE = 64 << 10;

    public static final int NETTY_HIGH_WATER_MARK = TRANSFER_BUFFER_SIZE;

    public static final int NETTY_LOW_WATER_MARK = NETTY_HIGH_WATER_MARK / 2;

    public static final int NETTY_RECEIVE_BUFFER_SIZE = TRANSFER_BUFFER_SIZE;

    private IOConfig() {}

    // KRYO


    // TODO: replace with static event size analysis
    public final static int MAX_EVENT_SIZE = TRANSFER_BUFFER_SIZE + 8 * 6 + // max. event meta data
            // (in transferbuffer)
            4 + // kryo class identifier
            4; // length field

    public static final int IO_EVENT_ID = 10;

    public static final int IO_DATA_EVENT_ID = 11;

    public static final int IO_TRANSFER_EVENT_ID = 12;
}
