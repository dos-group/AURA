package de.tuberlin.aura.core.common.eventsystem;


/**
 * The IEventListener interface declares the handler function.
 * @author Tobias Herb
 *
 */
public interface IEventHandler {

    /**
     * Declares the handler method for dispatched events.
     */
    void handleEvent( Event event );
}
