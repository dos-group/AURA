package de.tuberlin.aura.core.record.typeinfo;

import java.io.Serializable;

/**
 *
 */
public interface ITypeInformation extends Serializable {

    public abstract Class<?> getType();
}
