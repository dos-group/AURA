package de.tuberlin.aura.core.dataflow.datasets;

import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ImmutableDataset<E> extends AbstractDataset<E> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private List<E> data;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ImmutableDataset(final IExecutionContext context) {
        super(context);

        this.data = new ArrayList<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void add(E element) {
        // sanity check.
        if (element == null)
            throw new IllegalArgumentException("element == null");

        data.add(element);
    }

    @Override
    public Collection<E> getData() {
        return data;
    }

    @Override
    public void clear() {
        data.clear();
    }

    public void setData(final Collection<E> data) {
        // sanity check.
        if (data == null)
            throw new IllegalArgumentException("data == null");

        this.data = new ArrayList<>(data);
    }
}
