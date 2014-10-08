package de.tuberlin.aura.core.dataflow.udfs.contracts;

public interface IFoldFunction<I,O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract O empty();

    public abstract O singleton(final I in);

    public abstract O union(O currentValue, final O mRes);
}
