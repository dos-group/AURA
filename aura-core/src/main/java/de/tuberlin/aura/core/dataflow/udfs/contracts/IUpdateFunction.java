package de.tuberlin.aura.core.dataflow.udfs.contracts;

public interface IUpdateFunction<I,O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract O update(O state, final I update);

}
