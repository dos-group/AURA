package de.tuberlin.aura.core.operators;

import de.tuberlin.aura.core.common.utils.IVisitor;

/**
 *
 * @param <I>
 * @param <O>
 */
public final class MapPhysicalOperator<I,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MapPhysicalOperator(final OperatorProperties properties,
                               final IPhysicalOperator<I> inputOp,
                               final IUnaryUDFFunction<I, O> udfFunction) {

        super(properties, inputOp, udfFunction);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        inputOp.open();
    }

    @Override
    public O next() throws Throwable {
        final I input = inputOp.next();
        if (input != null)
            return udfFunction.apply(input);
        else
            return null;
    }

    @Override
    public void close() throws Throwable {
        inputOp.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
