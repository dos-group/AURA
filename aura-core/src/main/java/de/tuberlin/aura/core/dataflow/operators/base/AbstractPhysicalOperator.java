package de.tuberlin.aura.core.dataflow.operators.base;

/**
 *
 * @param <O>
 */
public abstract class AbstractPhysicalOperator<O> implements IPhysicalOperator<O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final IOperatorEnvironment environment;

    private boolean isOperatorOpen = false;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractPhysicalOperator(final IOperatorEnvironment environment) {

        this.environment = environment;
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
    public IOperatorEnvironment getEnvironment() {
        return environment;
    }

    @Override
    public boolean isOpen() {
        return isOperatorOpen;
    }
}
