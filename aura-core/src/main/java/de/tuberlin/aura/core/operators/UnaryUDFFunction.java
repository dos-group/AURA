package de.tuberlin.aura.core.operators;

/**
 *
 */
public interface UnaryUDFFunction<I,O> {

    public abstract O apply(final I in);
}