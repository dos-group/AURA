package de.tuberlin.aura.core.common.utils;

import java.io.Serializable;

/**
 * @param <A>
 * @param <B>
 */
public class Pair<A, B> implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1L;

    private final A first;

    private final B second;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    /**
     * @param first
     * @param second
     */
    public Pair(A first, B second) {
        super();
        this.first = first;
        this.second = second;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }

    @Override
    public int hashCode() {
        int hashFirst = first != null ? first.hashCode() : 0;
        int hashSecond = second != null ? second.hashCode() : 0;

        return (hashFirst + hashSecond) * hashSecond + hashFirst;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Pair) {
            @SuppressWarnings("unchecked")
            Pair<A, B> otherPair = (Pair<A, B>) other;
            return ((this.first == otherPair.first || (this.first != null && otherPair.first != null && this.first.equals(otherPair.first))) && (this.second == otherPair.second || (this.second != null
                    && otherPair.second != null && this.second.equals(otherPair.second))));
        }
        return false;
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }
}
