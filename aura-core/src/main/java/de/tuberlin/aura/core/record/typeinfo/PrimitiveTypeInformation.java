package de.tuberlin.aura.core.record.typeinfo;


import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class PrimitiveTypeInformation implements ITypeInformation {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final ITypeInformation byteTypeInfo       = new PrimitiveTypeInformation(byte.class);

    public static final ITypeInformation shortTypeInfo      = new PrimitiveTypeInformation(short.class);

    public static final ITypeInformation intTypeInfo        = new PrimitiveTypeInformation(int.class);

    public static final ITypeInformation longTypeInfo       = new PrimitiveTypeInformation(long.class);

    public static final ITypeInformation floatTypeInfo      = new PrimitiveTypeInformation(float.class);

    public static final ITypeInformation doubleTypeInfo     = new PrimitiveTypeInformation(double.class);

    public static final ITypeInformation charTypeInfo       = new PrimitiveTypeInformation(char.class);

    public static final ITypeInformation stringTypeInfo     = new PrimitiveTypeInformation(String.class);

    public static final ITypeInformation booleanTypeInfo    = new PrimitiveTypeInformation(boolean.class);


    public static final Set<ITypeInformation> primitiveTypes = new HashSet<>();

    static {

        primitiveTypes.add(byteTypeInfo);

        primitiveTypes.add(shortTypeInfo);

        primitiveTypes.add(intTypeInfo);

        primitiveTypes.add(longTypeInfo);

        primitiveTypes.add(floatTypeInfo);

        primitiveTypes.add(doubleTypeInfo);

        primitiveTypes.add(charTypeInfo);

        primitiveTypes.add(stringTypeInfo);

        primitiveTypes.add(booleanTypeInfo);
    }

    // ---------------------------------------------------
    // Fields.
    // --------------------------------------------------

    public final Class<?> type;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PrimitiveTypeInformation(final Class<?> type) {
        // sanity check.
        if (type == null)
            throw new IllegalArgumentException("type == null");

        this.type = type;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public Class<?> getType() {
        return type;
    }

    @Override
    public String toString() {
        return type.getSimpleName();
    }
}
