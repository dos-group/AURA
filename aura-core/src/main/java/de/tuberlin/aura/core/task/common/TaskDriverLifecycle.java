package de.tuberlin.aura.core.task.common;


public interface TaskDriverLifecycle {

    public abstract void startupDriver();

    public abstract void executeDriver();

    public abstract void teardownDriver(boolean awaitExhaustion);
}
