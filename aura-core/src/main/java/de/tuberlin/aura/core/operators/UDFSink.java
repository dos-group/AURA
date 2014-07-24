package de.tuberlin.aura.core.operators;

import de.tuberlin.aura.core.common.utils.IVisitor;

/**
 *
 * @param <I>
 */
public class UDFSink<I> extends AbstractUnaryUDFPhysicalOperator<I,Object> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UDFSink(final OperatorProperties properties, final IPhysicalOperator<I> inputOp, final IUnaryUDFFunction<I, Object> udfFunction) {
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
    public Void next() throws Throwable {
        final I input = inputOp.next();
        if (input != null)
            udfFunction.apply(input);
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
