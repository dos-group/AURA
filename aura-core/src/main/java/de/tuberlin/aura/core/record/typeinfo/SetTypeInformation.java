package de.tuberlin.aura.core.record.typeinfo;

import com.esotericsoftware.reflectasm.MethodAccess;

import java.util.Collection;
import java.util.Set;

/**
 *
 */
public final class SetTypeInformation extends AbstractCollectionTypeInformation {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final ITypeInformation _setTypeInformation = new PrimitiveTypeInformation(Set.class);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final MethodAccess methodAccess;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SetTypeInformation(final ITypeInformation genericType) {
        super(_setTypeInformation, genericType);

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
}
