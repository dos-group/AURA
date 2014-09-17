package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.TypeInformation;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

public class GroupByPhysicalOperator<I> extends AbstractUnaryPhysicalOperator<I,Collection<I>> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final TypeInformation inputTypeInfo;
    private I firstElementOfCurrentGroup;
    private boolean isEndOfStream;


    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public GroupByPhysicalOperator(IOperatorEnvironment environment, IPhysicalOperator<I> inputOp) {
        super(environment, inputOp);

        isEndOfStream = false;

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
    public Collection<I> next() throws Throwable {

        final List<I> group = new ArrayList<>();
        int[][] groupKeyIndices = getEnvironment().getProperties().groupByKeyIndices;

        if (isEndOfStream)
            return null;

        // first group
        if (firstElementOfCurrentGroup == null) {
            firstElementOfCurrentGroup = inputOp.next();

            // empty stream
            if (firstElementOfCurrentGroup == null) {
                return null;
            }
        }

        // determine the group's keys
        final ArrayList<Object> groupKeys = new ArrayList<>(groupKeyIndices.length);
        for (int i = 0; i < groupKeyIndices.length; i++) {
            groupKeys.add(i, inputTypeInfo.selectField(groupKeyIndices[i], firstElementOfCurrentGroup));
        }

        // add elements as long as their keys matches the group's keys
        I input = firstElementOfCurrentGroup;
        while (input != null) {

            group.add(input);

            input = inputOp.next();

            if (input == null) {
                isEndOfStream = true;
                return group;
            }

            for (int i = 0; i < groupKeyIndices.length; i++) {
                if (groupKeys.get(i) != inputTypeInfo.selectField(groupKeyIndices[i], input)) {
                    // the keys doesn't match the group's keys -> the group is finished with the previous element
                    firstElementOfCurrentGroup = input;
                    return group;
                }
            }

        }

        return group;
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
