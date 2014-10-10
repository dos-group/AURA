package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractBinaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.OperatorResult;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public final class UnionPhysicalOperator<I> extends AbstractBinaryPhysicalOperator<I,I,I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private boolean input1Selected;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UnionPhysicalOperator(final IExecutionContext context,
                                 final IPhysicalOperator<I> inputOp1,
                                 final IPhysicalOperator<I> inputOp2) {

        super(context, inputOp1, inputOp2);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();
        inputOp1.open();
        inputOp2.open();
    }

    @Override
    public OperatorResult<I> next() throws Throwable {

        OperatorResult<I> in = input1Selected ? inputOp1.next() : inputOp2.next();

        while (in.marker == StreamMarker.END_OF_STREAM_MARKER) {

            if (input1Selected) {
                inputOp1.close();
            } else {
                inputOp2.close();
            }

            if (!inputOp1.isOpen() && !inputOp2.isOpen()) {
                return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
            }

            input1Selected = !input1Selected;

            in = input1Selected ? inputOp1.next() : inputOp2.next();
        }

        if (inputOp1.isOpen() && inputOp2.isOpen()) {
            input1Selected = !input1Selected;
        }

        return in;
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
