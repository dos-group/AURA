package de.tuberlin.aura.core.record;

import java.util.Collection;
import java.util.Iterator;

public class GroupTypeInformation implements TypeInformation {

    // ---------------------------------------------------
    // Fields.
    // --------------------------------------------------

    final private TypeInformation elementType;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public GroupTypeInformation(TypeInformation elementType) {
        this.elementType = elementType;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public Class<?> getType() {
        return Collection.class;
    }

    @Override
    public Object selectField(final int[] selectorChain, final Object target) {

        Iterator<Collection> group = ((Collection) target).iterator();

        if (!group.hasNext()) {
            throw new IllegalStateException("Cannot select field in an empty group");
        }

        return elementType.selectField(selectorChain, group.next());
    }

    @Override
    public int[] buildFieldSelectorChain(final String accessPath) {
        return elementType.buildFieldSelectorChain(accessPath);
    }

}