package de.tuberlin.aura.core.operators;

import de.tuberlin.aura.core.common.utils.IVisitor;

/**
 *
 * @param <I>
 */
public final class UnionPhysicalOperator<I> extends AbstractBinaryPhysicalOperator<I,I,I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private boolean input1Closed = false;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UnionPhysicalOperator(final OperatorProperties properties,
                                 final IPhysicalOperator<I> inputOp1,
                                 final IPhysicalOperator<I> inputOp2) {

        super(properties, inputOp1, inputOp2);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        inputOp1.open();
    }

    @Override
    public I next() throws Throwable {

        if (!input1Closed) {
            final I input1 = inputOp1.next();

            if (input1 != null) {
                return input1;
            } else {
                inputOp1.close();
                input1Closed = true;
                inputOp2.open();
            }
        }

        return inputOp2.next();

        /*final I output;

        if (switchState)
          output = inputOp1.next();
        else
          output = inputOp2.next();

        return output;*/
    }

    @Override
    public void close() throws Throwable {
        inputOp2.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
