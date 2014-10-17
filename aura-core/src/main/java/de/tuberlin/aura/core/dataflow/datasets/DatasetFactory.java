package de.tuberlin.aura.core.dataflow.datasets;

import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;


public final class DatasetFactory {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    // Disallow Instantiation.
    private DatasetFactory() {}

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static AbstractDataset<?> createDataset(final IExecutionContext context) {
        // sanity check.
        if (context == null)
            throw new IllegalArgumentException("context == null");

        switch(context.getProperties().type) {
            case MUTABLE_DATASET:
                return new MutableDataset<>(context);
            case IMMUTABLE_DATASET:
                return new ImmutableDataset<>(context);
            case DATASET_REFERENCE:
                return new DatasetRef<>(context);
        }

        throw new IllegalStateException("'" + context.getProperties().type + "' is not defined.");
    }
}
