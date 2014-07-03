package de.tuberlin.aura.core.record.tuples;

import com.google.common.collect.ComparisonChain;

import java.util.Arrays;
import java.util.Iterator;

/**
 *
 */
public final class Tuple1<T0> extends AbstractTuple {

    private static final long serialVersionUID = -1L;

    public T0 _0;

    public Tuple1() {
        this((T0)null);
    }

    public Tuple1(final T0 _0) {
        this._0 = _0;
    }

    public Tuple1(final Tuple1<T0> t) {
        this(t._0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getField(final int pos) {
        switch(pos) {
            case 0: return (T) this._0;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void setField(final T value, final int pos) {
        switch(pos) {
            case 0:
                this._0 = (T0) value;
                break;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    public int length() {
        return 1;
    }

    @Override
    public Iterator<Object> iterator() {
        return Arrays.asList(new Object[]{_0}).iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(final AbstractTuple t) {
        if (this == t)
            return 0;
        final Tuple1<T0> o = (Tuple1<T0>)t;
        final ComparisonChain cc = ComparisonChain.start();
        if (_0 instanceof Comparable)
            cc.compare((Comparable<?>)_0, (Comparable<?>) o._0);
        return cc.result();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_0 == null) ? 0 : _0.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("unchecked")
        final Tuple1<T0> other = (Tuple1<T0>) obj;
        if (_0 == null) {
            if (other._0 != null)
                return false;
        } else if (!_0.equals(other._0))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "(" + _0 + ")";
    }
}
