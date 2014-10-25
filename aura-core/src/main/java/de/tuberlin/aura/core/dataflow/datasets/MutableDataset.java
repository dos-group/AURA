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

    public MutableDataset(final IExecutionContext environment) {
        super(environment);

        this.data = new HashMap<>();

        this.typeInfo = environment.getProperties().input1Type;

        this.datasetKeyIndices = environment.getProperties().datasetKeyIndices;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void add(final E element) {
        final Object[] keyFields = new Object[datasetKeyIndices.length];
        int fieldIndex = 0;
        for(final int[] selectorChain : datasetKeyIndices) {
            keyFields[fieldIndex++] = typeInfo.selectField(selectorChain, element);
        }
        data.put(keyFields, element);
    }

    public void update(final E element) {
        final Object[] keyFields = new Object[datasetKeyIndices.length];
        int fieldIndex = 0;
        for(final int[] selectorChain : datasetKeyIndices) {
            keyFields[fieldIndex++] = typeInfo.selectField(selectorChain, element);
        }
        data.put(keyFields, element);
    }

    // TODO: add a contains(final E element) {}

    @Override
    public Collection<E> getData() {
        return data.values();
    }
}
