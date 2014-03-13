package de.tuberlin.aura.core.task.common;

public interface TaskLifecycle {

    public abstract void create() throws Throwable;

    public abstract void open() throws Throwable;

    public abstract void run() throws Throwable;

    public abstract void close() throws Throwable;

    public abstract void release() throws Throwable;
}
