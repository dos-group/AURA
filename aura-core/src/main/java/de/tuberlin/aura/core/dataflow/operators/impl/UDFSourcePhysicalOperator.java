package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.contracts.ISourceFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;


public class UDFSourcePhysicalOperator<O> extends AbstractUnaryUDFPhysicalOperator<Object,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UDFSourcePhysicalOperator(final IExecutionContext context,
                                     final SourceFunction<O> function) {

        super(context, null, function);
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
