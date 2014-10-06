package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.descriptors.Descriptors;

import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class ExecutionContext implements IExecutionContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Descriptors.AbstractNodeDescriptor nodeDescriptor;

    private final Descriptors.NodeBindingDescriptor bindingDescriptor;

    private final Map<String,Class<?>> udfTypeMap;

    private final Map<UUID, Collection> datasets;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ExecutionContext(final Descriptors.AbstractNodeDescriptor nodeDescriptor,
                            final Descriptors.NodeBindingDescriptor bindingDescriptor) {
        // sanity check.
        if (nodeDescriptor == null)
            throw new IllegalArgumentException("nodeDescriptor == null");
        if (bindingDescriptor == null)
            throw new IllegalArgumentException("bindingDescriptor == null");

        this.nodeDescriptor = nodeDescriptor;

        this.bindingDescriptor = bindingDescriptor;

        this.udfTypeMap = new HashMap<>();

        this.datasets = new HashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

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

    @Override
    public void putUDFType(final String udfTypeName, final Class<?> udfType) {
        this.udfTypeMap.put(udfTypeName, udfType);
    }

    @Override
    public Class<?> getUDFType(final String udfTypeName) {
        return this.udfTypeMap.get(udfTypeName);
    }

    @Override
    public DataflowNodeProperties getProperties() {
        return nodeDescriptor.properties;
    }

    @Override
    public Descriptors.AbstractNodeDescriptor getNodeDescriptor() {
        return nodeDescriptor;
    }

    @Override
    public Descriptors.NodeBindingDescriptor getBindingDescriptor() {
        return bindingDescriptor;
    }
}
