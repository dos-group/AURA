package de.tuberlin.aura.core.common.utils;

/**
 *
 */
public interface Visitable<T> {

    public abstract void accept(final Visitor<T> visitor);
}
