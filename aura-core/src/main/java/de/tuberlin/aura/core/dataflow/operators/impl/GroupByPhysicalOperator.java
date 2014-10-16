package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.OperatorResult;
import de.tuberlin.aura.core.record.OperatorResult.StreamMarker;
import de.tuberlin.aura.core.record.TypeInformation;

import java.util.ArrayList;

public class GroupByPhysicalOperator<I> extends AbstractUnaryPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private TypeInformation inputTypeInfo;

    private int[][] groupKeyIndices;

    private OperatorResult<I> firstElementOfNewGroup;

    private ArrayList<Object> currentGroupKeys;


    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public GroupByPhysicalOperator(final IExecutionContext context,
                                   final IPhysicalOperator<I> inputOp) {

        super(context, inputOp);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {

        super.open();

        inputOp.open();

        inputTypeInfo = getContext().getProperties(this.getOperatorNum()).input1Type;

        groupKeyIndices = getContext().getProperties(this.getOperatorNum()).groupByKeyIndices;
    }

    @Override
    public OperatorResult<I> next() throws Throwable {

        OperatorResult<I> input;

        if (currentGroupKeys == null) { // start of a new group

            input = (firstElementOfNewGroup == null) ? inputOp.next() : firstElementOfNewGroup;

            if (input.marker == StreamMarker.END_OF_STREAM_MARKER) {
                return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
            }

            currentGroupKeys = new ArrayList<>(groupKeyIndices.length);

            for (int i = 0; i < groupKeyIndices.length; i++) {
                currentGroupKeys.add(i, inputTypeInfo.selectField(groupKeyIndices[i], input.element));
            }

            return input;

        } else { // (potential) continuation of a group
            input = inputOp.next();

            if (input.marker == StreamMarker.END_OF_STREAM_MARKER) {
                return input;
            }

            for (int i = 0; i < groupKeyIndices.length; i++) {
                if (!currentGroupKeys.get(i).equals(inputTypeInfo.selectField(groupKeyIndices[i], input.element))) {
                    // the group was finished with the previous element
                    firstElementOfNewGroup = input;
                    currentGroupKeys = null;

                    return new OperatorResult<>(StreamMarker.END_OF_GROUP_MARKER);
                }
            }

            return input;
        }
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
