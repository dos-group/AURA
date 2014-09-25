package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.TypeInformation;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

public class GroupByPhysicalOperator<I> extends AbstractUnaryPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final TypeInformation inputTypeInfo;
    private I firstElementOfNewGroup;
    private ArrayList<Object> groupKeys;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public GroupByPhysicalOperator(IOperatorEnvironment environment, IPhysicalOperator<I> inputOp) {
        super(environment, inputOp);

        this.inputTypeInfo = getEnvironment().getProperties().input1Type;
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

        int[][] groupKeyIndices = getEnvironment().getProperties().groupByKeyIndices;

        I input;

        // start of a new group
        if (groupKeys == null) {

            if (firstElementOfNewGroup == null) { // very first group
                input = inputOp.next();

                if (input == null) {
                    this.close();
                    return null;
                }
            } else { // another group
                input = firstElementOfNewGroup;
            }

            groupKeys = new ArrayList<>(groupKeyIndices.length);
            for (int i = 0; i < groupKeyIndices.length; i++) {
                groupKeys.add(i, inputTypeInfo.selectField(groupKeyIndices[i], input));
            }

            firstElementOfNewGroup = null;

            return input;

        } else { // (potential) continuation of a group
            input = inputOp.next();

            if (input == null) {
                this.close();
                return null;
            }

            for (int i = 0; i < groupKeyIndices.length; i++) {
                if (groupKeys.get(i) != inputTypeInfo.selectField(groupKeyIndices[i], input)) {
                    // the keys doesn't match the group's keys -> the group was finished with the previous element
                    groupKeys = null; // indicate that a group is finished
                    firstElementOfNewGroup = input; //

                    return null;
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
