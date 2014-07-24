package de.tuberlin.aura.core.operators;

import de.tuberlin.aura.core.common.utils.IVisitor;

/**
 *
 * @param <O>
 */
public class UDFSource<O> extends AbstractUnaryUDFPhysicalOperator<Object,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UDFSource(final OperatorProperties properties, final IUnaryUDFFunction<Object, O> udfFunction) {
        super(properties, null, udfFunction);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
    }

    @Override
    public O next() throws Throwable {
        return udfFunction.apply(null);
    }

    @Override
    public void close() throws Throwable {
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
