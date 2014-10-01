package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.descriptors.Descriptors;
import org.slf4j.Logger;

import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class OperatorEnvironment implements IOperatorEnvironment {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final DataflowNodeProperties properties;

    private final Descriptors.OperatorNodeDescriptor descriptor;

    private final Map<String,Class<?>> udfTypeMap;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public OperatorEnvironment(final DataflowNodeProperties properties,
                               final Descriptors.OperatorNodeDescriptor descriptor) {
        // sanity check.
        if (properties == null)
            throw new IllegalArgumentException("properties == null");
        if (descriptor == null)
            throw new IllegalArgumentException("descriptor == null");

        this.properties = properties;

        this.descriptor = descriptor;

        this.udfTypeMap = new HashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public DataflowNodeProperties getProperties() {
        return properties;
    }

    @Override
    public Descriptors.OperatorNodeDescriptor getNodeDescriptor() {
        return descriptor;
    }

    @Override
    public void putUdfType(String udfTypeName, Class<?> udfType) {
        this.udfTypeMap.put(udfTypeName, udfType);
    }

    @Override
    public Class<?> getUdfType(String udfTypeName) {
        return this.udfTypeMap.get(udfTypeName);
    }
}
