package de.tuberlin.aura.core.dataflow.operators.impl;

import java.util.Map;
import java.util.HashMap;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractBinaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.OperatorResult;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public class DifferencePhysicalOperator<I> extends AbstractBinaryPhysicalOperator<I,I,I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Map<I,Boolean> minusSideElements;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DifferencePhysicalOperator(final IExecutionContext context,
                                 final IPhysicalOperator<I> inputOp1,
                                 final IPhysicalOperator<I> inputOp2) {

        super(context, inputOp1, inputOp2);

        minusSideElements = new HashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();

        inputOp2.open();

        OperatorResult<I> in2 = inputOp2.next();

        while (in2.marker != StreamMarker.END_OF_STREAM_MARKER) {
            minusSideElements.put(in2.element, true);
            in2 = inputOp2.next();
        }

        inputOp2.close();
        inputOp1.open();
    }

    @Override
    public OperatorResult<I> next() throws Throwable {
        super.next();

        OperatorResult<I> in1 = inputOp1.next();

        while (in1.marker != StreamMarker.END_OF_STREAM_MARKER &&
                minusSideElements.containsKey(in1.element)) {

            in1 = inputOp1.next();
        }

        return in1;
    }

    @Override
    public void close() throws Throwable {
        super.close();
        inputOp1.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
