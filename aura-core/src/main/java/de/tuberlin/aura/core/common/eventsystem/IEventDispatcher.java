package de.tuberlin.aura.core.common.eventsystem;

/**
 * The EventDispatcher interface.
 *
 * @author Tobias Herb
 *
 */
public interface IEventDispatcher {

    /**
     * Add a listener for a specific event.
     * @param type The event type.
     * @param listener The handler for this event.
     */
    void addEventListener( String type, IEventHandler listener );

    /**
     * Remove a listener for a specific event.
     * @param type The event type.
     * @param listener The handler for this event.
     */
    boolean removeEventListener( String type, IEventHandler listener );

    /**
     * Dispatch a event.
     * @param event The event to dispatch.
     */
    public void dispatchEvent( Event event );

    /**
     * Checks if listeners are installed for that event type.
     * @param type The event type.
     * @return True if a listener is installed, else false.
     */
    public boolean hasEventListener( String type );
}
