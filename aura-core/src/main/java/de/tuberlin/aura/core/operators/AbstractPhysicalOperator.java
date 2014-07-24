package de.tuberlin.aura.core.operators;

/**
 *
 * @param <O>
 */
public abstract class AbstractPhysicalOperator<O> implements IPhysicalOperator<O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final OperatorProperties properties;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractPhysicalOperator(final OperatorProperties properties) {
        this.properties = properties;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
    }

    @Override
    public O next() throws Throwable {
        return null;
    }

    @Override
    public void close() throws Throwable {
    }

    @Override
    public OperatorProperties getProperties() {
        return properties;
    }
}
