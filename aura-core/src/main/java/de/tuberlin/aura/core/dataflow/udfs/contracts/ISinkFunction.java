package de.tuberlin.aura.core.dataflow.udfs.contracts;


public interface ISinkFunction<I> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void consume(final I in);
}
