package de.tuberlin.aura.core.dataflow.datasets;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;


import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;

public class ImmutableDataset<E> extends AbstractDataset<E> {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static String NUMBER_OF_CONSUMPTIONS = "NUMBER_OF_CONSUMPTIONS";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private List<E> data;

    private final boolean hasFixedNumberOfReads;

    private final int fixedNumberOfReads;

    private int readCount;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ImmutableDataset(final IExecutionContext context) {
        super(context);

        this.data = new LinkedList<>();

        if (context.getProperties(0).config != null && context.getProperties(0).config.containsKey(NUMBER_OF_CONSUMPTIONS)) {
            this.hasFixedNumberOfReads = true;
            fixedNumberOfReads = (int) context.getProperties(0).config.get(NUMBER_OF_CONSUMPTIONS);
        } else {
            this.hasFixedNumberOfReads = false;
            fixedNumberOfReads = -1;
        }

        readCount = 0;
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

        readCount++;

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

        this.data = new LinkedList<>(data);
    }

    public boolean hasFixedNumberOfReads() {
        return hasFixedNumberOfReads;
    }

    public boolean isLastRead() {
        return readCount + 1 == fixedNumberOfReads;
    }
}
