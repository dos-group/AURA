package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.functions.FoldFunction;
import de.tuberlin.aura.core.record.OperatorResult;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;

public class FoldPhysicalOperator<I,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Boolean isDrained;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public FoldPhysicalOperator(final IExecutionContext context,
                                final IPhysicalOperator<I> inputOp,
                                final FoldFunction<I, O> function) {

        super(context, inputOp, function);

        this.isDrained = false;
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

        if (this.isDrained) {
            return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
        }

        FoldFunction<I,O> function = ((FoldFunction<I,O>) this.function);

        O value = function.empty();

        OperatorResult<I> input = inputOp.next();

        if (input.marker == StreamMarker.END_OF_STREAM_MARKER) {
            return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
        }

        while (input.marker != StreamMarker.END_OF_GROUP_MARKER &&
                input.marker != StreamMarker.END_OF_STREAM_MARKER) {

            value = function.union(value, function.singleton(input.element));
            input = inputOp.next();
        }

        if (input.marker == StreamMarker.END_OF_STREAM_MARKER) {
            // finished last group
            this.isDrained = true;
        }

        return new OperatorResult<>(value);
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
