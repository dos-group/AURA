package de.tuberlin.aura.core.dataflow.operators.base;


public abstract class AbstractBinaryPhysicalOperator<I1,I2,O> extends AbstractPhysicalOperator<O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final IPhysicalOperator<I1> inputOp1;

    protected final IPhysicalOperator<I2> inputOp2;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractBinaryPhysicalOperator(final IOperatorEnvironment environment,
                                          final IPhysicalOperator<I1> inputOp1,
                                          final IPhysicalOperator<I2> inputOp2) {
        super(environment);

        // sanity check.
        if (inputOp1 == null)
            throw new IllegalArgumentException("input1 == null");
        if (inputOp2 == null)
            throw new IllegalArgumentException("input2 == null");

        this.inputOp1 = inputOp1;

        this.inputOp2 = inputOp2;
    }
}
