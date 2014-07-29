package de.tuberlin.aura.core.processing.operators.base;

import de.tuberlin.aura.core.processing.api.OperatorProperties;
import org.slf4j.Logger;

/**
 *
 */
public interface IOperatorEnvironment {

    public abstract Logger getLogger();

    public abstract OperatorProperties getProperties();
}
