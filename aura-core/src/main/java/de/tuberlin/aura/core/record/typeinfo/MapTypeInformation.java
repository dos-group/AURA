package de.tuberlin.aura.core.record.typeinfo;

import com.esotericsoftware.reflectasm.MethodAccess;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class MapTypeInformation implements ITypeInformation {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final ITypeInformation _mapTypeInformation = new PrimitiveTypeInformation(Map.class);

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ITypeInformation keyType;

    private final ITypeInformation valueType;

    private final MethodAccess methodAccess;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MapTypeInformation(final ITypeInformation keyType, final ITypeInformation valueType) {
        // sanity check.
        if (keyType == null)
            throw new IllegalArgumentException("keyType == null");
        if (valueType == null)
            throw new IllegalArgumentException("valueType == null");

        this.keyType = keyType;

        this.valueType = valueType;

        this.methodAccess = MethodAccess.get(getType());
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public Class<?> getType() {
        return _mapTypeInformation.getType();
    }

    public Class<?> getKeyType() {
        return keyType.getType();
    }

    public Class<?> getValueType() {
        return valueType.getType();
    }

    // ---------------------------------------------------
    // Collection Query Methods.
    // ---------------------------------------------------

    // Query Operations

    int size(final Object target) {
        return (Integer)methodAccess.invoke(target, "size");
    }

    boolean isEmpty(final Object target) {
        return (Boolean)methodAccess.invoke(target, "isEmpty");
    }

    boolean containsKey(final Object target, final Object key) {
        return (Boolean)methodAccess.invoke(target, "containsKey", key);
    }

    boolean containsValue(final Object target, final Object value) {
        return (Boolean)methodAccess.invoke(target, "containsValue", value);
    }

    Object get(final Object target, final Object key) {
        return methodAccess.invoke(target, "get", key);
    }

    // Views

    Set<?> keySet(final Object target) {
        return (Set<?>)methodAccess.invoke(target, "keySet");
    }

    Collection<?> values(final Object target) {
        return (Collection<?>)methodAccess.invoke(target, "values");
    }

    Set<Map.Entry<?, ?>> entrySet(final Object target) {
        return (Set<Map.Entry<?, ?>>)methodAccess.invoke(target, "entrySet");
    }
}
