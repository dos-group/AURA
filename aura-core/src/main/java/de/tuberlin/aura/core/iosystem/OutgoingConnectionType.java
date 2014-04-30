package de.tuberlin.aura.core.iosystem;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;

/**
 * 
 * @param <T>
 */
public interface OutgoingConnectionType<T> {

    Bootstrap bootStrap(EventLoopGroup eventLoopGroup);
}
