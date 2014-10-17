package de.tuberlin.aura.core.common.eventsystem;


public interface IEventDispatcher {

    public abstract void addEventListener(String type, IEventHandler listener);

    public abstract void addEventListener(final String[] types, final IEventHandler listener);

    public abstract boolean removeEventListener(String type, IEventHandler listener);

    public abstract void removeAllEventListener();

    public abstract void dispatchEvent(Event event);

    public abstract boolean hasEventListener(String type);

    public abstract void joinDispatcherThread();

    public abstract void shutdownEventDispatcher();
}
