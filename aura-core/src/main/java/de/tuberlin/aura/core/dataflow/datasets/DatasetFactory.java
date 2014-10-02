package de.tuberlin.aura.core.dataflow.datasets;

import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;


public final class DatasetFactory {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    // Disallow Instantiation.
    private DatasetFactory() {}

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static AbstractDataset<?> createDataset(final IOperatorEnvironment environment) {
        // sanity check.
        if (environment == null)
            throw new IllegalArgumentException("environment == null");

        switch(environment.getProperties().type) {
            case MUTABLE_DATASET:
                return new MutableDataset<>(environment);
            case IMMUTABLE_DATASET:
                return new ImmutableDataset<>(environment);
        }
        throw new IllegalStateException("'" + environment.getProperties().type + "' is not defined.");
    }
}
