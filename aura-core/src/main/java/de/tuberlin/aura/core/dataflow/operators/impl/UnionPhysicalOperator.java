package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractBinaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;

/**
 *
 * @param <I>
 */
public final class UnionPhysicalOperator<I> extends AbstractBinaryPhysicalOperator<I,I,I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private boolean selectedInput;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UnionPhysicalOperator(final IOperatorEnvironment environment,
                                 final IPhysicalOperator<I> inputOp1,
                                 final IPhysicalOperator<I> inputOp2) {

        super(environment, inputOp1, inputOp2);
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
    public I next() throws Throwable {

        I in = null;

        if (selectedInput)
            in = inputOp1.next();
        else
            in = inputOp2.next();

        if (in == null) {

            if (selectedInput) {
                inputOp1.close();
            } else {
                inputOp2.close();
            }

            selectedInput = !selectedInput;
        }

        if (inputOp1.isOpen() && inputOp2.isOpen())
            selectedInput = !selectedInput;

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
