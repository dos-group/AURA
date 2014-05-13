package de.tuberlin.aura.taskmanager.factories;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import de.tuberlin.aura.core.memory.BufferMemoryManager;
import de.tuberlin.aura.core.memory.spi.IBufferMemoryManager;
import de.tuberlin.aura.core.task.spi.*;
import de.tuberlin.aura.taskmanager.*;

/**
 *
 */
public final class TaskManagerModule extends AbstractModule {

    @Override
    protected void configure() {

        install(new FactoryModuleBuilder()
                .implement(IBufferMemoryManager.class, BufferMemoryManager.class)
                .build(TaskManagerFactories.IBufferMemoryManagerFactory.class));

        install(new FactoryModuleBuilder()
                .implement(IDataConsumer.class, TaskDataConsumer.class)
                .build(TaskManagerFactories.IDataConsumerFactory.class));

        install(new FactoryModuleBuilder()
                .implement(IDataProducer.class, TaskDataProducer.class)
                .build(TaskManagerFactories.IDataProducerFactory.class));

        install(new FactoryModuleBuilder()
                .implement(ITaskDriver.class, TaskDriver.class)
                .build(TaskManagerFactories.ITaskDriverFactory.class));

        install(new FactoryModuleBuilder()
                .implement(ITaskExecutionManager.class, TaskExecutionManager.class)
                .build(TaskManagerFactories.ITaskExecutionManagerFactory.class));

        install(new FactoryModuleBuilder()
                .implement(ITaskExecutionUnit.class, TaskExecutionUnit.class)
                .build(TaskManagerFactories.ITaskExecutionUnitFactory.class));

        install(new FactoryModuleBuilder()
                .implement(ITaskManager.class, TaskManager.class)
                .build(TaskManagerFactories.ITaskManagerFactory.class));
    }
}
