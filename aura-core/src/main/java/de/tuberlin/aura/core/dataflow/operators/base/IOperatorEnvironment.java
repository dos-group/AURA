package de.tuberlin.aura.core.dataflow.operators.base;

import java.io.Serializable;
import java.util.Collection;
import java.util.UUID;

import org.slf4j.Logger;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowNodeProperties;


public interface IOperatorEnvironment extends Serializable {

    public abstract DataflowNodeProperties getProperties();

    public abstract Descriptors.AbstractNodeDescriptor getNodeDescriptor();

    public abstract void putUDFType(final String udfTypeName, final Class<?> udfType);

    public abstract Class<?> getUDFType(final String udfTypeName);

    public abstract <E> void putDataset(final UUID uid, final Collection<E> dataset);

    public abstract <E> Collection<E> getDataset(final UUID uid);
}
