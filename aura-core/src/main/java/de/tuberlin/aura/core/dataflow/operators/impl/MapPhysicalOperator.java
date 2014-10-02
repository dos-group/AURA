package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.contracts.IMapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction;


public final class MapPhysicalOperator<I,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MapPhysicalOperator(final IOperatorEnvironment environment,
                               final IPhysicalOperator<I> inputOp,
                               final MapFunction<I, O> function) {

        super(environment, inputOp, function);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();
        inputOp.open();
    }

    @Override
    public O next() throws Throwable {
        final I input = inputOp.next();
        if (input != null)
            return ((IMapFunction<I,O>)function).map(input);
        else
            return null;
    }

    @Override
    public void close() throws Throwable {
        super.close();
        inputOp.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
