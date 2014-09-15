package de.tuberlin.aura.core.record.typeinfo;

/**
 *
 */
public abstract class AbstractCollectionTypeInformation implements ITypeInformation {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ITypeInformation collectionType;

    private final ITypeInformation genericType;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractCollectionTypeInformation(final ITypeInformation collectionType, final ITypeInformation genericType) {
        // sanity check.
        if (collectionType == null)
            throw new IllegalArgumentException("collectionType == null");
        if (genericType == null)
            throw new IllegalArgumentException("genericType == null");

        this.collectionType = collectionType;

        this.genericType = genericType;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public Class<?> getType() {
        return collectionType.getType();
    }

    public Class<?> getGenericType() {
        return genericType.getType();
    }
}
