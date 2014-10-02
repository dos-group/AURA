package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.descriptors.Descriptors;

import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class OperatorEnvironment implements IOperatorEnvironment {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Descriptors.AbstractNodeDescriptor descriptor;

    private final Map<String,Class<?>> udfTypeMap;

    private final Map<UUID, Collection> datasets;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public OperatorEnvironment(final Descriptors.AbstractNodeDescriptor descriptor) {
        // sanity check.
        if (descriptor == null)
            throw new IllegalArgumentException("descriptor == null");

        this.descriptor = descriptor;

        this.udfTypeMap = new HashMap<>();

        this.datasets = new HashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public DataflowNodeProperties getProperties() {
        return descriptor.properties;
    }

    @Override
    public Descriptors.AbstractNodeDescriptor getNodeDescriptor() {
        return descriptor;
    }

    @Override
    public void putUDFType(final String udfTypeName, final Class<?> udfType) {
        this.udfTypeMap.put(udfTypeName, udfType);
    }

    @Override
    public Class<?> getUDFType(final String udfTypeName) {
        return this.udfTypeMap.get(udfTypeName);
    }

    @Override
    public <E> void putDataset(final UUID uid, final Collection<E> dataset) {
        // sanity check.
        if (uid == null)
            throw new IllegalArgumentException("uid == null");
        if (dataset == null)
            throw new IllegalArgumentException("dataset == null");

        datasets.put(uid, dataset);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> Collection<E> getDataset(final UUID uid) {
        // sanity check.
        if (uid == null)
            throw new IllegalArgumentException("uid == null");

        return datasets.get(uid);
    }
}
