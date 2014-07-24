package de.tuberlin.aura.core.operators;

/**
 *
 * @param <I>
 * @param <O>
 */
public abstract class AbstractUnaryUDFPhysicalOperator<I,O> extends AbstractUnaryPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final IUnaryUDFFunction<I,O> udfFunction;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    AbstractUnaryUDFPhysicalOperator(final OperatorProperties properties, final IPhysicalOperator<I> inputOp, final IUnaryUDFFunction<I, O> udfFunction) {
        super(properties, inputOp);

        this.udfFunction = udfFunction;
    }
}
