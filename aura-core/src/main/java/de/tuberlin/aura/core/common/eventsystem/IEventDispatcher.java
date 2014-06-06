package de.tuberlin.aura.core.common.eventsystem;

/**
 * The EventDispatcher interface.
 * 
 * @author Tobias Herb
 */
public interface IEventDispatcher {

    /**
     * Add a listener for a specific event.
     * 
     * @param type The event type.
     * @param listener The handler for this event.
     */
    public abstract void addEventListener(String type, IEventHandler listener);

    /**
     *
     * @param types
     * @param listener
     */
    public abstract void addEventListener(final String[] types, final IEventHandler listener);

    /**
     * Remove a listener for a specific event.
     * 
     * @param type The event type.
     * @param listener The handler for this event.
     */
    public abstract boolean removeEventListener(String type, IEventHandler listener);

    /**
     *
     */
    public abstract void removeAllEventListener();

    /**
     * Dispatch a event.
     * 
     * @param event The event to dispatch.
     */
    public abstract void dispatchEvent(Event event);

    /**
     * Checks if listeners are installed for that event type.
     * 
     * @param type The event type.
     * @return True if a listener is installed, else false.
     */
    public abstract boolean hasEventListener(String type);

    /**
     *
     */
    public abstract void joinDispatcherThread();

    /**
     * Shutdown the event dispatcher cleanly.
     */
    public abstract void shutdown();
}
