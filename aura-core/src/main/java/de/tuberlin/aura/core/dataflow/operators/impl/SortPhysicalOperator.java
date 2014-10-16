package de.tuberlin.aura.core.dataflow.operators.impl;

import java.util.Comparator;
import java.util.PriorityQueue;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.OperatorResult;
import de.tuberlin.aura.core.record.TypeInformation;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public final class SortPhysicalOperator<I> extends AbstractUnaryPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public class SortComparator<T> implements Comparator<OperatorResult<T>> {

        private DataflowNodeProperties properties = getContext().getProperties();

        @Override
        public int compare(final OperatorResult<T> o1, final OperatorResult<T> o2) {

            for(final int[] selectorChain : getContext().getProperties().sortKeyIndices) {

                final Comparable f1 = (Comparable)inputType.selectField(selectorChain, o1.element);
                final Comparable f2 = (Comparable)inputType.selectField(selectorChain, o2.element);
                final int res = properties.sortOrder == DataflowNodeProperties.SortOrder.ASCENDING ? f1.compareTo(f2) : f2.compareTo(f1);

                if (res != 0)
                    return res;
            }

            return 0;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final TypeInformation inputType;

    private PriorityQueue<OperatorResult<I>> priorityQueue;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SortPhysicalOperator(final IExecutionContext context,
                                final IPhysicalOperator<I> inputOp) {

        super(context, inputOp);

        this.inputType = getContext().getProperties(getOperatorNum()).input1Type;

        this.priorityQueue = new PriorityQueue<>(10, new SortComparator<I>());
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();

        inputOp.open();

        OperatorResult<I> in = inputOp.next();

        while (in.marker != StreamMarker.END_OF_STREAM_MARKER) {
            priorityQueue.add(in);
            in = inputOp.next();
        }

        inputOp.close();
    }

    @Override
    public OperatorResult<I> next() throws Throwable {
        OperatorResult<I> result = priorityQueue.poll();
        return (result != null) ? result : new OperatorResult<I>(StreamMarker.END_OF_STREAM_MARKER);
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
