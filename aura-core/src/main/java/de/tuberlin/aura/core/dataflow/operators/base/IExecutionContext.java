package de.tuberlin.aura.core.dataflow.operators.base;

import java.io.Serializable;
import java.util.Collection;
import java.util.UUID;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;


public interface IExecutionContext extends Serializable {

    public abstract DataflowNodeProperties getProperties();

    public abstract DataflowNodeProperties getProperties(int operatorNum);

    public abstract Descriptors.AbstractNodeDescriptor getNodeDescriptor();

    public abstract Descriptors.NodeBindingDescriptor getBindingDescriptor();

    public abstract void putUDFType(final String udfTypeName, final Class<?> udfType);

    public abstract Class<?> getUDFType(final String udfTypeName);

    public abstract <E> void putDataset(final UUID uid, final Collection<E> dataset);

    public abstract <E> Collection<E> getDataset(final UUID uid);

    public abstract ITaskRuntime getRuntime();

    public abstract void put(final String name, final Object obj);

    public abstract Object get(final String name);
}
