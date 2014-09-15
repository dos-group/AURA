package de.tuberlin.aura.core.record.typeinfo;

import com.esotericsoftware.reflectasm.MethodAccess;

import java.util.Collection;
import java.util.List;

/**
 *
 */
public final class ListTypeInformation extends AbstractCollectionTypeInformation {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final ITypeInformation _listTypeInformation = new PrimitiveTypeInformation(List.class);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final MethodAccess methodAccess;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ListTypeInformation(final ITypeInformation genericType) {
        super(_listTypeInformation, genericType);

        this.methodAccess = MethodAccess.get(getType());
    }

    // ---------------------------------------------------
    // Collection Query Methods.
    // ---------------------------------------------------

    // Query Operations

    public int size(final Object target) {
        return (Integer)methodAccess.invoke(target, "size");
    }

    public boolean isEmpty(final Object target) {
        return (Boolean)methodAccess.invoke(target, "isEmpty");
    }

    public boolean contains(final Object target, final Object o) {
        return (Boolean)methodAccess.invoke(target, "contains", o);
    }

    public boolean containsAll(final Object target, final Collection c) {
        return (Boolean)methodAccess.invoke(target, "containsAll", c);
    }

    public Object get(final Object target, final int index) {
        return methodAccess.invoke(target, "get", index);
    }

    public int indexOf(final Object target, final Object o) {
        return (Integer)methodAccess.invoke(target, "indexOf", o);
    }

    public int lastIndexOf(final Object target, final Object o) {
        return (Integer)methodAccess.invoke(target, "lastIndexOf", o);
    }
}
