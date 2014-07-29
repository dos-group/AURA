package de.tuberlin.aura.core.processing.operators.base;

import org.slf4j.Logger;

import de.tuberlin.aura.core.processing.api.OperatorProperties;

/**
 *
 */
public interface IOperatorEnvironment {

    public abstract Logger getLogger();

    public abstract OperatorProperties getProperties();
}
