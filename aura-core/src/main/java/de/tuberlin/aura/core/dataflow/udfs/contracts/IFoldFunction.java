package de.tuberlin.aura.core.dataflow.udfs.contracts;

public interface IFoldFunction<I,O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract O empty();

    public abstract O singleton(final I element);

    public abstract O union(O result, final O element);
}
