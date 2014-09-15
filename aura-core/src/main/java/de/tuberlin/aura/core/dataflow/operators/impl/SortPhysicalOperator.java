package de.tuberlin.aura.core.dataflow.operators.impl;

import java.util.Comparator;
import java.util.PriorityQueue;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.TypeInformation;

/**
 *
 */
public final class SortPhysicalOperator<I> extends AbstractUnaryPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public class SortComparator<T> implements Comparator<T> {

        private DataflowNodeProperties properties = getEnvironment().getProperties();

        @Override
        public int compare(final T o1, final T o2) {
            for(final int[] selectorChain : getEnvironment().getProperties().sortKeyIndices) {
                final Comparable f1 = (Comparable)inputType.selectField(selectorChain, o1);
                final Comparable f2 = (Comparable)inputType.selectField(selectorChain, o2);
                final int res = properties.sortOrder == DataflowNodeProperties.SortOrder.ASCENDING ? f1.compareTo(f2) : f2.compareTo(f1);
                if(res != 0)
                    return res;
            }
            return 0;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final TypeInformation inputType;

    private PriorityQueue<I> priorityQueue;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SortPhysicalOperator(final IOperatorEnvironment environment,
                                final IPhysicalOperator<I> inputOp) {

        super(environment, inputOp);

        this.inputType = getEnvironment().getProperties().input1Type;

        this.priorityQueue = new PriorityQueue<>(10, new SortComparator<I>());
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();
        I in = null;
        inputOp.open();
        in = inputOp.next();
        while (in != null) {
            priorityQueue.add(in);
            in = inputOp.next();
        }
        inputOp.close();
    }

    @Override
    public I next() throws Throwable {
        return priorityQueue.poll();
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
