package de.tuberlin.aura.core.dataflow.operators.impl;

import java.util.*;

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

    public class SortComparator<T> implements Comparator<T> {

        private DataflowNodeProperties properties = getContext().getProperties();

        @Override
        @SuppressWarnings("unchecked")
        public int compare(final T o1, final T o2) {

            for(final int[] selectorChain : getContext().getProperties().sortKeyIndices) {

                final Comparable f1 = (Comparable)inputType.selectField(selectorChain, o1);
                final Comparable f2 = (Comparable)inputType.selectField(selectorChain, o2);
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

    private List<I> elements;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SortPhysicalOperator(final IExecutionContext context,
                                final IPhysicalOperator<I> inputOp) {

        super(context, inputOp);

        this.inputType = getContext().getProperties(getOperatorNum()).input1Type;

        this.elements = new LinkedList<>();
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
            elements.add(in.element);
            in = inputOp.next();
        }

        inputOp.close();

        Collections.sort(elements, new SortComparator<>());
    }

    @Override
    public OperatorResult<I> next() throws Throwable {
        if (elements.size() > 0)
            return new OperatorResult<>(elements.remove(0));
        else
            return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
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
