package de.tuberlin.aura.core.operators;

/**
 *
 * @param <I>
 * @param <O>
 */
public abstract class AbstractUnaryPhysicalOperator<I,O> extends AbstractPhysicalOperator<O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final IPhysicalOperator<I> inputOp;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    AbstractUnaryPhysicalOperator(final OperatorProperties properties, final IPhysicalOperator<I> inputOp) {
        super(properties);
        this.inputOp = inputOp;
    }
}
