package de.tuberlin.aura.taskmanager.factories;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.memory.BufferAllocatorGroup;
import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.memory.spi.IBufferMemoryManager;
import de.tuberlin.aura.core.task.spi.*;

/**
 *
 */
public class TaskManagerFactories {

    // Disallow instantiation.
    private TaskManagerFactories() {
    }

    public static interface IBufferMemoryManagerFactory {

        IBufferMemoryManager create(final Descriptors.MachineDescriptor machineDescriptor);
    }

    public static interface IDataConsumerFactory {

        public abstract IDataConsumer create(final ITaskDriver taskDriver,
                                             final IAllocator allocator);
    }

    public static interface IDataProducerFactory {

        public abstract IDataProducer create(final ITaskDriver taskDriver,
                                             final IAllocator outputAllocator);
    }

    public static interface ITaskDriverFactory {

        public abstract ITaskDriver create(final ITaskManager taskManager,
                                           final Descriptors.TaskDeploymentDescriptor deploymentDescriptor);
    }

    public static interface ITaskExecutionManagerFactory {

        public abstract ITaskExecutionManager create(final Descriptors.MachineDescriptor machineDescriptor,
                                                     final IBufferMemoryManager bufferMemoryManager);
    }

    public interface ITaskExecutionUnitFactory {

        public abstract ITaskExecutionUnit create(final ITaskExecutionManager executionManager,
                                                  final int executionUnitID,
                                                  final BufferAllocatorGroup inputAllocator,
                                                  final BufferAllocatorGroup outputAllocator);
    }

    public interface ITaskManagerFactory {

        public abstract ITaskManager create(final String zookeeperAddress,
                                            final int controlPort,
                                            final int dataPort,
                                            final ITaskExecutionManager taskExecutionManagerFactory,
                                            final IBufferMemoryManagerFactory bufferMemoryManagerFactory);
    }
}
