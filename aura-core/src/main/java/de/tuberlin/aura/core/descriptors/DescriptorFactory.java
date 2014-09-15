package de.tuberlin.aura.core.descriptors;

import java.net.InetAddress;

import de.tuberlin.aura.core.common.utils.InetHelper;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.descriptors.Descriptors.HDDDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.HardwareDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;

/**
 *
 */
public final class DescriptorFactory {

    // Disallow instantiation.
    private DescriptorFactory() {}

    public static MachineDescriptor createMachineDescriptor(IConfig config) {

        // Get the IP address of this node.
        InetAddress address = InetHelper.getIPAddress();

        // Get information about the hardware of this machine.
        int dataPort = config.getInt("io.tcp.port");
        int controlPort = config.getInt("io.rpc.port");
        int cpuCores = config.getInt("machine.cpu.cores");
        long memoryMax = config.getLong("machine.memory.max");
        long diskSize = config.getLong("machine.disk.size");

        // Sanity check
        if (dataPort < 1024 || dataPort > 65535)
            throw new IllegalArgumentException(String.format("Invalid value %d for 'io.tcp.port'", dataPort));
        if (controlPort < 1024 || controlPort > 65535)
            throw new IllegalArgumentException(String.format("Invalid value %d for 'io.rpc.port'", controlPort));

        // Construct a new hardware descriptor
        HardwareDescriptor hardware = new HardwareDescriptor(cpuCores, memoryMax, new HDDDescriptor(diskSize));
        // Construct a new machine descriptor
        return new MachineDescriptor(address, dataPort, controlPort, hardware);
    }
}
