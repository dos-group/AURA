package de.tuberlin.aura.core.common.utils;

import java.io.IOException;
import java.net.*;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InetHelper {

    private static final Set<Integer> reservedPorts = new HashSet<>();

    private static final Logger LOG = LoggerFactory.getLogger(InetHelper.class);

    public static InetAddress getIPAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

            while (interfaces.hasMoreElements()) {
                NetworkInterface curInterface = interfaces.nextElement();

                // Don't use the loopback interface.
                if (curInterface.isLoopback()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = curInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress curAddress = addresses.nextElement();

                    // Check only for IPv4 addresses.
                    if (curAddress instanceof Inet4Address) {
                        return curAddress;
                    }
                }
            }
        } catch (SocketException e) {
            LOG.error("I/O error", e);
        }

        return null;
    }

    public static int getFreePort() {
        int freePort = -1;
        do {
            try {
                final ServerSocket ss = new ServerSocket(0);
                freePort = ss.getLocalPort();
                ss.close();
            } catch (IOException e) {
                LOG.info(e.getLocalizedMessage(), e);
            }
        } while (reservedPorts.contains(freePort) || freePort < 1024 || freePort > 65535);
        reservedPorts.add(freePort);
        return freePort;
    }
}
