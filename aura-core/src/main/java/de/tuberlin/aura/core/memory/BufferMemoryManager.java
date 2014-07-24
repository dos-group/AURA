package de.tuberlin.aura.core.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import de.tuberlin.aura.core.config.IConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.memory.spi.IBufferMemoryManager;

/**
 *
 */
public final class BufferMemoryManager implements IBufferMemoryManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(BufferMemoryManager.class);

    private final Descriptors.MachineDescriptor machineDescriptor;

    private final IConfig config;

    private final Runtime runtime;

    private final long maxMemory;

    private final int globalBufferCount;

    private final List<BufferAllocatorGroup> allocatorGroups;

    private final AtomicInteger allocatorIndex;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public BufferMemoryManager(final Descriptors.MachineDescriptor machineDescriptor, IConfig config) {
        // sanity check.
        if (machineDescriptor == null)
            throw new IllegalArgumentException("machineDescriptor == null");

        this.machineDescriptor = machineDescriptor;

        this.config = config;

        final int bufferSize = config.getInt("memory.buffer.size");
        final double bufferLoadFactor = config.getDouble("memory.load.factor");
        final int numOfAllocatorsPerGroup = config.getInt("memory.group.allocators");
        final int groupsPerExecutionUnit = config.getInt("memory.groups.per.execution.unit");

        this.runtime = Runtime.getRuntime();

        this.maxMemory = runtime.maxMemory();

        final int numOfExecutionUnits = machineDescriptor.hardware.cpuCores;

        this.globalBufferCount = (int) ((maxMemory * bufferLoadFactor) / bufferSize);

        final int perExecutionUnitBuffers = globalBufferCount / numOfExecutionUnits;

        final int buffersPerAllocator = (perExecutionUnitBuffers / groupsPerExecutionUnit) / numOfAllocatorsPerGroup;

        this.allocatorGroups =
                setupBufferAllocatorGroups(numOfExecutionUnits * groupsPerExecutionUnit,
                                           numOfAllocatorsPerGroup,
                                           buffersPerAllocator,
                                           bufferSize);

        this.allocatorIndex = new AtomicInteger(0);

        LOG.debug("Execution Units: {}", numOfExecutionUnits);
        LOG.debug("Max Memory: {}", maxMemory);
        LOG.debug("Buffer Count: {}", globalBufferCount);
        LOG.debug("IAllocator Groups: {} with {} allocators each", allocatorGroups.size(), numOfAllocatorsPerGroup);
        LOG.debug("Groups per Execution Unit: {}", groupsPerExecutionUnit);
        LOG.debug("Buffers Per IAllocator: {}", buffersPerAllocator);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public long getUsedMemory() {
        return runtime.totalMemory() - runtime.freeMemory();
    }

    public long getFreeMemory() {
        return runtime.freeMemory();
    }

    public long getTotalMemory() {
        return runtime.totalMemory();
    }

    public BufferAllocatorGroup getBufferAllocatorGroup() {
        return allocatorGroups.get(allocatorIndex.getAndIncrement() % allocatorGroups.size());
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private List<BufferAllocatorGroup> setupBufferAllocatorGroups(final int numOfAllocatorGroups,
                                                                  final int numOfAllocatorsPerGroup,
                                                                  final int buffersPerAllocator,
                                                                  final int bufferSize) {

        final List<BufferAllocatorGroup> allocatorGroups = new ArrayList<>();
        for (int i = 0; i < numOfAllocatorGroups; ++i) {
            final List<IAllocator> initialAllocators = new ArrayList<>();

            for (int j = 0; j < numOfAllocatorsPerGroup; ++j)
                initialAllocators.add(new BufferAllocator(bufferSize, buffersPerAllocator));

            allocatorGroups.add(new BufferAllocatorGroup(bufferSize, initialAllocators));
        }
        return allocatorGroups;
    }
}
