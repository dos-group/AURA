package de.tuberlin.aura.core.record.tuples;

import java.io.Serializable;
import java.util.Iterator;

/**
*
*/
public abstract class AbstractTuple
        implements Serializable, Iterable<Object>, Comparable<AbstractTuple> {

    private static final long serialVersionUID = -1L;

    public abstract <T> T getField(final int pos);

    public abstract <T> void setField(final T value, final int pos);

    public abstract int length();

    public abstract Iterator<Object> iterator();

    public abstract int compareTo(final AbstractTuple o);
}
