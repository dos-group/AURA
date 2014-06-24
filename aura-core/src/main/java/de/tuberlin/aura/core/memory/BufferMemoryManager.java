package de.tuberlin.aura.core.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.memory.spi.IBufferMemoryManager;

/**
 *
 */
public final class BufferMemoryManager implements IBufferMemoryManager {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final int BUFFER_SIZE = BufferAllocator._64K;

    public static final double BUFFER_LOAD_FACTOR = 0.01;

    public static final int NUM_OF_ALLOCATORS_PER_GROUP = 2;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(BufferMemoryManager.class);

    private final Descriptors.MachineDescriptor machineDescriptor;

    private final Runtime runtime;

    private final long maxMemory;

    private final int globalBufferCount;

    private final List<BufferAllocatorGroup> allocatorGroups;

    private final AtomicInteger allocatorIndex;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public BufferMemoryManager(final Descriptors.MachineDescriptor machineDescriptor) {
        // sanity check.
        if (machineDescriptor == null)
            throw new IllegalArgumentException("machineDescriptor == null");

        this.machineDescriptor = machineDescriptor;

        this.runtime = Runtime.getRuntime();

        this.maxMemory = runtime.maxMemory();

        final int numOfExecutionUnits = machineDescriptor.hardware.cpuCores;

        final int groupsPerExecutionUnit = 2;

        this.globalBufferCount = (int) ((maxMemory * BUFFER_LOAD_FACTOR) / BUFFER_SIZE);

        final int perExecutionUnitBuffers = globalBufferCount / numOfExecutionUnits;

        final int buffersPerAllocator = (perExecutionUnitBuffers / groupsPerExecutionUnit) / NUM_OF_ALLOCATORS_PER_GROUP;

        this.allocatorGroups =
                setupBufferAllocatorGroups(numOfExecutionUnits * groupsPerExecutionUnit,
                                           NUM_OF_ALLOCATORS_PER_GROUP,
                                           buffersPerAllocator,
                                           BufferAllocator._64K);

        this.allocatorIndex = new AtomicInteger(0);

        LOG.debug("Execution Units: {}", numOfExecutionUnits);
        LOG.debug("Max Memory: {}", maxMemory);
        LOG.debug("Buffer Count: {}", globalBufferCount);
        LOG.debug("IAllocator Groups: {} with {} allocators each", allocatorGroups.size(), NUM_OF_ALLOCATORS_PER_GROUP);
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
