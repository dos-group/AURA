package de.tuberlin.aura.core.memory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.descriptors.Descriptors;

/**
 *
 */
public final class MemoryManager {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);

    // Disallow instantiation.
    private MemoryManager() {}

    /**
     *
     */
    public static final class MemoryView {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        public final Allocator allocator;

        public final byte[] memory;

        public final int baseOffset;

        public final int size;

        // private final AtomicInteger refCount;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public MemoryView(final Allocator allocator, final byte[] memory) {
            this(allocator, memory, 0, memory.length);
        }

        public MemoryView(final Allocator allocator, final byte[] memory, int baseOffset, int size) {
            // sanity check.
            if (allocator == null)
                throw new IllegalArgumentException("allocator == null");
            if (memory == null)
                throw new IllegalArgumentException("memory == null");
            if (baseOffset < 0 && baseOffset >= memory.length)
                throw new IllegalArgumentException("bad baseOffset");
            if (size < 0 && size >= memory.length && baseOffset + size >= memory.length)
                throw new IllegalArgumentException("bad size");

            this.allocator = allocator;

            this.memory = memory;

            this.baseOffset = baseOffset;

            this.size = size;

            // this.refCount = new AtomicInteger(0);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public int size() {
            return size;
        }

        public byte[] copy() {
            return Arrays.copyOfRange(memory, baseOffset, baseOffset + size);
        }

        public void copy(byte[] dst) {
            // sanity check.
            if (dst == null)
                throw new IllegalArgumentException("dst == null");
            System.arraycopy(memory, baseOffset, dst, 0, baseOffset + size);
        }

        public void free() {
            allocator.free(this);
        }


        /*
         * public MemoryView weakCopy() { acquire(); return this; }
         * 
         * public void acquire() { refCount.getAndIncrement(); }
         * 
         * public void release() { refCount.getAndDecrement(); }
         * 
         * public int getRefCount() { return refCount.get(); }
         */
    }

    /**
     *
     */
    public static final class BufferMemoryManager {

        // ---------------------------------------------------
        // Constants.
        // ---------------------------------------------------

        public static final int BUFFER_SIZE = BufferAllocator._64K;

        public static final double BUFFER_LOAD_FACTOR = 0.1;

        // TODO: Change back to 4
        public static final int NUM_OF_ALLOCATORS_PER_GROUP = 1;

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private final Descriptors.MachineDescriptor machineDescriptor;

        private final Runtime runtime;

        private final long maxMemory;

        private final int globalBufferCount;

        // private final ThreadLocal<BufferAllocatorGroup> threadBufferAllocator;

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
            LOG.debug("Allocator Groups: {} with {} allocators each", allocatorGroups.size(), NUM_OF_ALLOCATORS_PER_GROUP);
            LOG.debug("Groups per Execution Unit: {}", groupsPerExecutionUnit);
            LOG.debug("Buffers Per Allocator: {}", buffersPerAllocator);

            // this.threadBufferAllocator = new ThreadLocal<BufferAllocatorGroup>() {

            // @Override
            // protected BufferAllocatorGroup initialValue() {
            // return allocatorGroups.get(allocatorIndex.getAndIncrement() %
            // allocatorGroups.size());
            // }
            // };
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
                final List<Allocator> initialAllocators = new ArrayList<>();

                for (int j = 0; j < numOfAllocatorsPerGroup; ++j)
                    initialAllocators.add(new BufferAllocator(bufferSize, buffersPerAllocator));

                allocatorGroups.add(new BufferAllocatorGroup(bufferSize, initialAllocators));
            }
            return allocatorGroups;
        }
    }

    /**
     *
     */
    public static interface Allocator {

        public abstract MemoryView alloc();

        public abstract void free(final MemoryView memory);

        public abstract boolean hasFree();

        public abstract int getBufferSize();

        public abstract boolean isNotUsed();
    }

    /**
     *
     */
    public static final class BufferAllocatorGroup implements Allocator {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private final int bufferSize;

        final List<Allocator> assignedAllocators;

        // ---------------------------------------------------
        // Constants.
        // ---------------------------------------------------

        public BufferAllocatorGroup(final int bufferSize, final List<Allocator> initialAssignedAllocators) {
            // sanity check.
            if ((bufferSize & (bufferSize - 1)) != 0 && bufferSize < BufferAllocator._8K && bufferSize > BufferAllocator._64K)
                throw new IllegalArgumentException("illegal buffer size");
            if (initialAssignedAllocators == null)
                throw new IllegalArgumentException("initialAssignedAllocators == null");

            // check correct buffer sizes.
            for (final Allocator allocator : initialAssignedAllocators) {
                if (allocator.getBufferSize() != bufferSize)
                    throw new IllegalStateException("buffer size of allocator does not fit");
            }

            this.bufferSize = bufferSize;

            this.assignedAllocators = initialAssignedAllocators;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public void addAllocator(final Allocator allocator) {
            // sanity check.
            if (allocator == null)
                throw new IllegalArgumentException("allocator == null");
            if (allocator.getBufferSize() != bufferSize)
                throw new IllegalArgumentException("buffer size does not fit");

            assignedAllocators.add(allocator);
        }

        public Allocator removeAllocator() {
            Allocator notUsedAllocator = null;
            for (final Allocator allocator : assignedAllocators) {
                if (allocator.isNotUsed()) {
                    notUsedAllocator = allocator;
                    break;
                }
            }
            return notUsedAllocator;
        }

        public Allocator getAllocator(final int index) {
            // sanity check.
            if (index < 0)
                throw new IndexOutOfBoundsException("index < 0");
            if (index >= assignedAllocators.size())
                throw new IndexOutOfBoundsException("index < 0");

            return assignedAllocators.get(index);
        }

        // ---------------------------------------------------
        // Public Methods: Allocator Interface.
        // ---------------------------------------------------

        @Override
        public MemoryView alloc() {
            for (final Allocator allocator : assignedAllocators) {
                if (allocator.hasFree()) {
                    return allocator.alloc();
                }
            }

            LOG.trace("No buffers available anymore -> blocking wait");
            MemoryView memory = assignedAllocators.get(0).alloc();

            return memory;
        }

        @Override
        public void free(MemoryView memory) {
            memory.free();
        }

        @Override
        public int getBufferSize() {
            return bufferSize;
        }

        @Override
        public boolean hasFree() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNotUsed() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     *
     */
    public static final class BufferAllocator implements Allocator {

        // ---------------------------------------------------
        // Constants.
        // ---------------------------------------------------

        public static final int _8K = 1024 * 8;

        public static final int _16K = 1024 * 16;

        public static final int _32K = 1024 * 32;

        public static final int _64K = 1024 * 64;

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private final int bufferSize;

        private final int bufferCount;

        private final byte[] memoryArena;

        private final BlockingQueue<MemoryView> freeList;

        // private final Set<MemoryView> usedSet;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public BufferAllocator(final int bufferSize, final int bufferCount) {

            // sanity check.
            if ((bufferSize & (bufferSize - 1)) != 0 && bufferSize < _8K && bufferSize > _64K)
                throw new IllegalArgumentException("illegal buffer size");
            if (bufferCount <= 0)
                throw new IllegalArgumentException("bufferCount <= 0");
            if ((bufferCount * bufferSize) % _64K != 0)
                throw new IllegalArgumentException("allocated memory must be a multiple of 64K");

            this.bufferSize = bufferSize;

            this.bufferCount = bufferCount;

            this.memoryArena = new byte[bufferSize * bufferCount];

            this.freeList = new LinkedBlockingQueue<>();

            // this.usedSet = new HashSet<>();

            for (int i = 0; i < bufferCount; ++i) {
                this.freeList.add(new MemoryView(this, memoryArena, i * bufferSize, bufferSize));
            }
        }

        // ---------------------------------------------------
        // Public Methods: Allocator Interface.
        // ---------------------------------------------------

        @Override
        public MemoryView alloc() {
            try {
                final MemoryView memory = freeList.take();

                // memory.acquire();
                // usedSet.add(memory);
                return memory;
            } catch (InterruptedException ie) {
                return null;
            }
        }

        @Override
        public void free(final MemoryView memory) {
            // sanity check.
            if (memory == null)
                throw new IllegalArgumentException("memory == null");

            // memory.release();
            // if(memory.getRefCount() == 0) {
            freeList.add(memory);
            // usedSet.remove(memory);
            // }
        }

        @Override
        public boolean hasFree() {
            return freeList.size() > 0;
        }

        @Override
        public int getBufferSize() {
            return bufferSize;
        }

        @Override
        public boolean isNotUsed() {
            return false;// usedSet.isEmpty();
        }
    }

    // --------------------- TESTING ---------------------

    public static void main(String[] arg) {

        // final BufferMemoryManager mm = new
        // BufferMemoryManager(DescriptorFactory.createMachineDescriptor(14124, 12232));
    }
}
