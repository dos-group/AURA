package de.tuberlin.aura.core.processing.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.processing.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.processing.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.processing.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.processing.udfs.contracts.ISourceFunction;
import de.tuberlin.aura.core.processing.udfs.functions.SourceFunction;

/**
 *
 * @param <O>
 */
public class UDFSourcePhysicalOperator<O> extends AbstractUnaryUDFPhysicalOperator<Object,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UDFSourcePhysicalOperator(final IOperatorEnvironment environment,
                                     final SourceFunction<O> function) {

        super(environment, null, function);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();
    }

    @Override
    public O next() throws Throwable {
        return ((ISourceFunction<O>)function).produce();
    }

    @Override
    public void close() throws Throwable {
        super.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
