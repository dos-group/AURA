package de.tuberlin.aura.core.descriptors;

import java.io.File;
import java.net.InetAddress;

import de.tuberlin.aura.core.common.utils.InetHelper;
import de.tuberlin.aura.core.descriptors.Descriptors.HDDDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.HardwareDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;

/**
 * Singleton.
 *
 * @author Christian Wuertz
 */
public final class DescriptorFactory {

        /**
         * Do not allow instances of this class.
         */
        private DescriptorFactory() {
        }

        /**
         * Builds the {@link MachineDescriptor} for the machine this processes is running on.
         *
         * @return A {@link MachineDescriptor}.
         */
        public static MachineDescriptor getDescriptor(int dataPort, int controlPort)
        {
                // Get the IP address of this node.
                InetAddress address = InetHelper.getIPAddress();

                // TODO: The ports should be set by a config file.

                // Get information about the hardware of this machine.
                int cpuCores = Runtime.getRuntime().availableProcessors();
                long sizeOfRAM = Runtime.getRuntime().maxMemory();

                // TODO: Specify a temp directory by a config file.
                long sizeOfHDD = new File("/").getTotalSpace();
                HDDDescriptor hdd = new HDDDescriptor(sizeOfHDD);

                HardwareDescriptor hardware = new HardwareDescriptor(cpuCores, sizeOfRAM, hdd);

                return new MachineDescriptor(address, dataPort, controlPort, hardware);
        }
}
