package de.tuberlin.aura.core.common.utils;

/**
 *
 */
public interface Visitor<T> {

    public abstract void visit(final T element);
}