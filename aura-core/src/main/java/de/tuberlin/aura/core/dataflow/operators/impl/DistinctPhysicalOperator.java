package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.OperatorResult;

import java.util.Map;
import java.util.HashMap;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public class DistinctPhysicalOperator<I> extends AbstractUnaryPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    Map<I,Boolean> hashes;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistinctPhysicalOperator(final IExecutionContext context,
                                    final IPhysicalOperator<I> inputOp) {

        super(context, inputOp);

        hashes = new HashMap<>();
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
    public OperatorResult<I> next() throws Throwable {

        OperatorResult<I> input = inputOp.next();

        while (input.marker != StreamMarker.END_OF_STREAM_MARKER &&
                hashes.containsKey(input.element)) {

            input = inputOp.next();
        }

        hashes.put(input.element, true);

        return input;
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
