package de.tuberlin.aura.demo.examples;

import com.google.common.collect.Lists;
import de.tuberlin.aura.core.memory.test.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public final class SimpleClient2 {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(SimpleClient2.class);

    // Disallow Instantiation.
    private SimpleClient2() {}

    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        List<IAllocator> allocators = new ArrayList<>();
        allocators.add(new BufferAllocator(BufferAllocator._64K, 1));
        allocators.add(new BufferAllocator(BufferAllocator._64K, 1));
        allocators.add(new BufferAllocator(BufferAllocator._64K, 1));
        allocators.add(new BufferAllocator(BufferAllocator._64K, 1));

        final BufferAllocatorGroup ba = new BufferAllocatorGroup(BufferAllocator._64K, allocators);

        final MemoryView buffer1 = ba.alloc();

        final MemoryView buffer2 = ba.alloc();

        final MemoryView buffer3 = ba.alloc();

        final MemoryView buffer4 = ba.alloc();

        final MemoryView buffer5 = ba.alloc(new BufferCallback() {

            @Override
            public void bufferReader(MemoryView buffer) {

                LOG.info("RECEIVED BUFFER 1");

            }
        });

        final MemoryView buffer6 = ba.alloc(new BufferCallback() {

            @Override
            public void bufferReader(MemoryView buffer) {

                LOG.info("RECEIVED BUFFER 2");

            }
        });

        ba.free(buffer1);

        ba.free(buffer2);

        ba.free(buffer3);

        final MemoryView buffer7 = ba.alloc(new BufferCallback() {

            @Override
            public void bufferReader(MemoryView buffer) {

                LOG.info("RECEIVED BUFFER 3");

            }
        });


        final MemoryView buffer8 = ba.alloc(new BufferCallback() {

            @Override
            public void bufferReader(MemoryView buffer) {

                LOG.info("RECEIVED BUFFER 4");

            }
        });

        ba.free(buffer4);
    }
}
