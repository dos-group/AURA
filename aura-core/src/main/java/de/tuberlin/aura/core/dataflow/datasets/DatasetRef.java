package de.tuberlin.aura.core.dataflow.datasets;

import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;

import java.util.Collection;


public class DatasetRef<E> extends AbstractDataset<E> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private AbstractDataset<E> internalDataset;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DatasetRef(final IExecutionContext context, final AbstractDataset<E> internalDataset) {
        super(context);

        // sanity check.
        if (internalDataset == null)
            throw new IllegalArgumentException("internalDataset == null");

        this.internalDataset = internalDataset;
    }

    public DatasetRef(final IExecutionContext context) {
        this(context, new ImmutableDataset<E>(context));
    }

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    @Override
    public void add(E element) {
        internalDataset.add(element);
    }

    @Override
    public Collection<E> getData() {
        return internalDataset.getData();
    }

    @Override
    public void clear() {
        internalDataset.clear();
    }

    @Override
    public void setData(final Collection<E> data) {
        internalDataset.setData(data);
    }

    public void assignDataset(final AbstractDataset<E> internalDataset) {
        // sanity check.
        if (internalDataset == null)
            throw new IllegalArgumentException("internalDataset == null");

        this.internalDataset.setData(internalDataset.getData());
    }
}
