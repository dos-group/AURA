package de.tuberlin.aura.core.dataflow.operators.base;


import java.util.ArrayList;
import java.util.List;

public abstract class AbstractPhysicalOperator<O> implements IPhysicalOperator<O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final IExecutionContext environment;

    private boolean isOperatorOpen = false;

    private List<Integer> outputGateIndices;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractPhysicalOperator(final IExecutionContext environment) {
        // sanity check.
        if (environment == null)
            throw new IllegalArgumentException("environment == null");

        this.environment = environment;

        this.outputGateIndices = new ArrayList<>();

        for (int gateIndex = 0; gateIndex < environment.getBindingDescriptor().outputGateBindings.size(); ++gateIndex)
            outputGateIndices.add(gateIndex);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        this.isOperatorOpen = true;
    }

    @Override
    public O next() throws Throwable {
        return null;
    }

    @Override
    public void close() throws Throwable {
        this.isOperatorOpen = false;
    }

    @Override
    public IExecutionContext getContext() {
        return environment;
    }

    @Override
    public boolean isOpen() {
        return isOperatorOpen;
    }

    @Override
    public void setOutputGates(final List<Integer> outputGateIndices) {
        this.outputGateIndices = outputGateIndices;
    }

    @Override
    public List<Integer> getOutputGates() {
        return outputGateIndices;
    }
}
