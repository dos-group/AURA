package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.TypeInformation;

import java.util.ArrayList;

public class GroupByPhysicalOperator<I> extends AbstractUnaryPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final TypeInformation inputTypeInfo;

    private I firstElementOfNewGroup;

    private ArrayList<Object> currentGroupKeys;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public GroupByPhysicalOperator(IExecutionContext environment, IPhysicalOperator<I> inputOp) {
        super(environment, inputOp);

        this.inputTypeInfo = getContext().getProperties().input1Type;
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
    public I next() throws Throwable {

        int[][] groupKeyIndices = getContext().getProperties().groupByKeyIndices;

        I input;

        // start of a new group
        if (currentGroupKeys == null) {

            if (firstElementOfNewGroup == null) { // very first group
                input = inputOp.next();

                if (input == null) { // empty stream
                    this.close();
                    return null;
                }
            } else {
                input = firstElementOfNewGroup;
            }

            currentGroupKeys = new ArrayList<>(groupKeyIndices.length);
            for (int i = 0; i < groupKeyIndices.length; i++) {
                currentGroupKeys.add(i, inputTypeInfo.selectField(groupKeyIndices[i], input));
            }

            firstElementOfNewGroup = null;

            return input;

        } else { // (potential) continuation of a group
            input = inputOp.next();

            if (input == null) { // finished stream
                this.close();
                return null;
            }

            for (int i = 0; i < groupKeyIndices.length; i++) {
                if (currentGroupKeys.get(i) != inputTypeInfo.selectField(groupKeyIndices[i], input)) {
                    // the group was finished with the previous element
                    firstElementOfNewGroup = input;
                    currentGroupKeys = null;
                    return null; // indicate that a group is finished
                }
            }

            return input;
        }
    }

    @Override
    public void close() throws Throwable {
        inputOp.close();
        super.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }

}
