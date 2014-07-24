package de.tuberlin.aura.core.operators;

/**
 *
 * @param <I1>
 * @param <I2>
 * @param <O>
 */
public abstract class AbstractBinaryPhysicalOperator<I1,I2,O> extends AbstractPhysicalOperator<O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final IPhysicalOperator<I1> inputOp1;

    protected final IPhysicalOperator<I2> inputOp2;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    AbstractBinaryPhysicalOperator(final OperatorProperties properties, final IPhysicalOperator<I1> inputOp1, final IPhysicalOperator<I2> inputOp2) {
        super(properties);

        // sanity check.
        if (inputOp1 == null)
            throw new IllegalArgumentException("inputOp1 == null");
        if (inputOp2 == null)
            throw new IllegalArgumentException("inputOp2 == null");

        this.inputOp1 = inputOp1;

        this.inputOp2 = inputOp2;
    }
}
