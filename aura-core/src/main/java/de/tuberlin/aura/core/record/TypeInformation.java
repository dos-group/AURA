package de.tuberlin.aura.core.record;

import java.io.Serializable;

public interface TypeInformation extends Serializable {

    Class<?> getType();

    Object selectField(int[] selectorChain, Object target);

    int[] buildFieldSelectorChain(String accessPath);
}
