package de.tuberlin.aura.core.record;

import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.typeinfo.GroupEndMarker;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class GroupedOperatorInputIterator<I> implements Iterator<I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final IPhysicalOperator<I> inputOperator;

    private I next;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public GroupedOperatorInputIterator(IPhysicalOperator<I> inputOperator) {
        this.inputOperator = inputOperator;

        try {
            next = inputOperator.next();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    @Override
    public I next() {
        try {

            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            I result = next;

            next = inputOperator.next();

            return result;

        } catch (Throwable e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
        return !isDrained() && !isGroupFinished();
    }

    public boolean isDrained() {
        return next == null;
    }

    public boolean isGroupFinished() {
        return next == GroupEndMarker.class;
    }
}
