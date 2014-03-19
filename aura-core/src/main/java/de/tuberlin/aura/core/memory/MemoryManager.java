package de.tuberlin.aura.core.memory;

import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 */
public final class MemoryManager {

    private static final Logger LOG = Logger.getLogger(MemoryManager.class);

    // Disallow instantiation.
    private MemoryManager() {
    }

    /**
     *
     */
    public static final class Memory {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        public final Allocator allocator;

        public final byte[] memory;

        public final int baseOffset;

        public final int size;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public Memory(final Allocator allocator, final byte[] memory) {
            this(allocator, memory, 0, memory.length);
        }

        public Memory(final Allocator allocator, final byte[] memory, int baseOffset, int size) {
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
    }

    /**
     *
     */
    public static final class BufferMemoryManager {

        // ---------------------------------------------------
        // Constants.
        // ---------------------------------------------------

        public static final int BUFFER_SIZE = BufferAllocator._64K;

        public static final double BUFFER_LOAD_FACTOR = 0.7;

        public static final int ALLOCATORS_PER_GROUP = 4;

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private final Descriptors.MachineDescriptor machineDescriptor;

        private final Runtime runtime;

        private final long maxMemory;

        private final int globalBufferCount;

        public final ThreadLocal<Allocator> threadBufferAllocator;

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

            this.globalBufferCount = (int) ((maxMemory * BUFFER_LOAD_FACTOR) / BUFFER_SIZE);

            final int perExecutionUnitBuffers = globalBufferCount / machineDescriptor.hardware.cpuCores;

            final int buffersPerAllocator = perExecutionUnitBuffers / ALLOCATORS_PER_GROUP;

            this.threadBufferAllocator = new ThreadLocal<Allocator>() {

                @Override
                protected Allocator initialValue() {
                    final List<Allocator> initialAllocators = new ArrayList<>();
                    for (int i = 0; i < ALLOCATORS_PER_GROUP; ++i) {
                        initialAllocators.add(new BufferAllocator(BUFFER_SIZE, buffersPerAllocator));
                    }
                    return new BufferAllocatorGroup(BUFFER_SIZE, initialAllocators);
                }
            };
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

        public Allocator getAllocator() {
            return threadBufferAllocator.get();
        }
    }

    /**
     *
     */
    public static interface Allocator {

        public abstract Memory alloc();

        public abstract void free(final Memory memory);

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

        // ---------------------------------------------------
        // Public Methods: Allocator Interface.
        // ---------------------------------------------------

        @Override
        public Memory alloc() {
            for (final Allocator allocator : assignedAllocators) {
                if (allocator.hasFree())
                    return allocator.alloc();
            }
            return assignedAllocators.get(0).alloc();
        }

        @Override
        public void free(Memory memory) {
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

        private final BlockingQueue<Memory> freeList;

        private final Set<Memory> usedSet;

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

            this.usedSet = new HashSet<>();

            for (int i = 0; i < bufferCount - 1; ++i) {
                this.freeList.add(new Memory(this, memoryArena, i * bufferSize, bufferSize));
            }
        }

        // ---------------------------------------------------
        // Public Methods: Allocator Interface.
        // ---------------------------------------------------

        @Override
        public Memory alloc() {
            try {
                final Memory memory = freeList.take();
                usedSet.add(memory);
                return memory;
            } catch (InterruptedException ie) {
                return null;
            }
        }

        @Override
        public void free(final Memory memory) {
            // sanity check.
            if (memory == null)
                throw new IllegalArgumentException("memory == null");
            freeList.add(memory);
            usedSet.remove(memory);
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
            return usedSet.isEmpty();
        }


        public int freeBuffer() {
            return freeList.size();
        }

    }

    // --------------------- TESTING ---------------------

    public static void main(String[] arg) {

        final BufferMemoryManager mm = new BufferMemoryManager(DescriptorFactory.createMachineDescriptor(14124, 12232));
    }
}
