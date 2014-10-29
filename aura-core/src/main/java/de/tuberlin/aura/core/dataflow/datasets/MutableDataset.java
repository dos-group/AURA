package de.tuberlin.aura.core.dataflow.datasets;

import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.record.TypeInformation;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MutableDataset<E> extends AbstractDataset<E> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Map<Object[],E> data;

    private final TypeInformation typeInfo;

    private final int[][] datasetKeyIndices;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MutableDataset(final IExecutionContext context) {
        super(context);

        this.data = new HashMap<>();

        this.typeInfo = context.getProperties().input1Type;

        this.datasetKeyIndices = context.getProperties().datasetKeyIndices;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void add(final E element) {
        data.put(getKeyFields(element), element);
    }

    @Override
    public void clear() {
        data.clear();
    }

    @Override
    public void setData(final Collection<E> data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<E> getData() {
        return data.values();
    }

    public E get(final Object[] keys) {
        return data.get(keys);
    }

    public void update(final E element) {
        update(getKeyFields(element), element);
    }

    public void update(final Object[] keys, final E element) {
        data.put(keys, element);
    }

    public boolean containsElement(final Object[] keys) {
        return data.containsKey(keys);
    }

    public boolean containsElement(final E element) {
        return data.containsKey(getKeyFields(element));
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private Object[] getKeyFields(E element) {
        final Object[] keyFields = new Object[datasetKeyIndices.length];
        int fieldIndex = 0;
        for(final int[] selectorChain : datasetKeyIndices) {
            keyFields[fieldIndex++] = typeInfo.selectField(selectorChain, element);
        }
        return keyFields;
    }
}
