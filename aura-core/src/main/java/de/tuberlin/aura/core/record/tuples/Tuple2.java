package de.tuberlin.aura.core.record.tuples;

import com.google.common.collect.ComparisonChain;

import java.util.Arrays;
import java.util.Iterator;

/**
 *
 */
public final class Tuple2<T0, T1> extends AbstractTuple {

    private static final long serialVersionUID = -1L;

    public T0 _0;

    public T1 _1;

    public Tuple2() {
        this(null, null);
    }

    public Tuple2(final T0 _0, final T1 _1) {

        this._0 = _0;

        this._1 = _1;
    }

    public Tuple2(final Tuple2<T0, T1> t) {
        this(t._0, t._1);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getField(final int pos) {
        switch(pos) {
            case 0: return (T) this._0;
            case 1: return (T) this._1;
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
            case 1:
                this._1 = (T1) value;
                break;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    public int length() {
        return 2;
    }

    @Override
    public Iterator<Object> iterator() {
        return Arrays.asList(new Object[]{_0, _1}).iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(final AbstractTuple t) {
        if (this == t)
            return 0;
        final Tuple2<T0,T1> o = (Tuple2<T0,T1>)t;
        final ComparisonChain cc = ComparisonChain.start();
        if (_0 instanceof Comparable)
            cc.compare((Comparable<?>)_0, (Comparable<?>) o._0);
        if (_1 instanceof Comparable)
            cc.compare((Comparable<?>)_1, (Comparable<?>) o._1);
        return cc.result();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_0 == null) ? 0 : _0.hashCode());
        result = prime * result + ((_1 == null) ? 0 : _1.hashCode());
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
        final Tuple2<T0,T1> other = (Tuple2<T0,T1>) obj;
        if (_0 == null) {
            if (other._0 != null)
                return false;
        } else if (!_0.equals(other._0))
            return false;
        if (_1 == null) {
            if (other._1 != null)
                return false;
        } else if (!_1.equals(other._1))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "(" + _0 + "," + _1 + ")";
    }
}
