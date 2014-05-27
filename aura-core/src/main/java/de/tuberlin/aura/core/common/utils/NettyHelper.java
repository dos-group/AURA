package de.tuberlin.aura.core.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class NettyHelper {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(NettyHelper.class);

    public static LengthFieldBasedFrameDecoder getLengthDecoder() {
        return new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4);
    }
}
