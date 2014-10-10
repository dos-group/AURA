package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.contracts.IMapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction;
import de.tuberlin.aura.core.record.OperatorResult;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public final class MapPhysicalOperator<I,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MapPhysicalOperator(final IExecutionContext context,
                               final IPhysicalOperator<I> inputOp,
                               final MapFunction<I, O> function) {

        super(context, inputOp, function);
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
    public OperatorResult<O> next() throws Throwable {

        final OperatorResult<I> input = inputOp.next();

        if (input.marker != StreamMarker.END_OF_STREAM_MARKER) {
            return new OperatorResult<>(((IMapFunction<I,O>)function).map(input.element));
        } else {
            return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
        }
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
