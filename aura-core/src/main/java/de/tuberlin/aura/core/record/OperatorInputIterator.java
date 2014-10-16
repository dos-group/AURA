package de.tuberlin.aura.core.record;

import java.util.Iterator;
import java.util.NoSuchElementException;


import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;

public class OperatorInputIterator<I> implements Iterator<I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final IPhysicalOperator<I> inputOperator;

    private OperatorResult<I> next;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public OperatorInputIterator(IPhysicalOperator<I> inputOperator) {
        this.inputOperator = inputOperator;

        try {
            next = inputOperator.next();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public I next() {
        try {

            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            OperatorResult<I> result = next;

            next = inputOperator.next();

            return result.element;

        } catch (Throwable e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return !endOfGroup() &&
                !endOfStream();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public boolean endOfGroup() {
        return next.marker == StreamMarker.END_OF_GROUP_MARKER;
    }

    public boolean endOfStream() {
        return next.marker == StreamMarker.END_OF_STREAM_MARKER;
    }
}
