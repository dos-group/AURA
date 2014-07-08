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

    /**
     * Builds the {@link MachineDescriptor} for the machine this processes is running on.
     * 
     * @param config The config instance for parameter lookup.
     * @param ns The namespace under which config values will be found (one of wm, tm, or client).
     * @return A {@link MachineDescriptor}.
     */
    public static MachineDescriptor createMachineDescriptor(IConfig config, String ns) {

        // Get the IP address of this node.
        InetAddress address = InetHelper.getIPAddress();

        // Get information about the hardware of this machine.
        int dataPort = config.getInt(String.format("%s.io.tcp.port", ns));
        int controlPort = config.getInt(String.format("%s.io.rpc.port", ns));
        int cpuCores = config.getInt(String.format("%s.machine.cpu.cores", ns));
        long memoryMax = config.getLong(String.format("%s.machine.memory.max", ns));
        long diskSize = config.getLong(String.format("%s.machine.disk.size", ns));

        // Sanity check
        if (dataPort < 1024 || dataPort > 65535)
            throw new IllegalArgumentException(String.format("Invalid value %d for '%s.io.tcp.port'", dataPort, ns));
        if (controlPort < 1024 || controlPort > 65535)
            throw new IllegalArgumentException(String.format("Invalid value %d for '%s.io.rpc.port'", controlPort, ns));

        // Construct a new hardware descriptor
        HardwareDescriptor hardware = new HardwareDescriptor(cpuCores, memoryMax, new HDDDescriptor(diskSize));
        // Construct a new machine descriptor
        return new MachineDescriptor(address, dataPort, controlPort, hardware);
    }
}
