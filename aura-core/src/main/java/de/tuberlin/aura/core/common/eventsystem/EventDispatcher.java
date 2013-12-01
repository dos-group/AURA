package de.tuberlin.aura.core.common.eventsystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The EventDispatcher is responsible for adding and removing listeners and
 * for dispatching event to this listeners. This EventDispatcher implementation is not thread safe!
 *
 * @author Tobias Herb
 *
 */
public class EventDispatcher implements IEventDispatcher {

    /**
     * Constructor.
     */
    public EventDispatcher() {
        this.listenerMap = new ConcurrentHashMap<String, List<IEventHandler>>();
    }

    /** Hold the listeners for a specific event. */
    final Map<String,List<IEventHandler>> listenerMap;

    /**
     * Add a listener for a specific event.
     * @param type The event type.
     * @param listener The handler for this event.
     */
    @Override
    public void addEventListener( String type, IEventHandler listener ) {
        // sanity check.
        if( type == null )
            throw new NullPointerException();
        if( listener == null )
            throw new NullPointerException();

        List<IEventHandler> listeners = listenerMap.get( type );
        if( listeners == null ) {
            listeners = new ArrayList<IEventHandler>();
            listenerMap.put( type, listeners );
        }
        listeners.add( listener );
    }

    /**
     * Remove a listener for a specific event.
     * @param type The event type.
     * @param listener The handler for this event.
     */
    @Override
    public boolean removeEventListener( String type, IEventHandler listener ) {
        // sanity check.
        if( type == null )
            throw new NullPointerException();
        if( listener == null )
            throw new NullPointerException();

        final List<IEventHandler> listeners = listenerMap.get( type );
        if( listeners != null ) {
            boolean isRemoved = listeners.remove( listener );
            // if no more listeners registered, we remove the complete mapping.
            if( isRemoved && listeners.size() == 0 ) {
                listenerMap.remove( type );
            }
            return isRemoved;
        } else {
            return false;
        }
    }

    /**
     * Dispatch a event.
     * @param event The event to dispatch.
     */
    @Override
    public void dispatchEvent( Event event ) {
        // sanity check.
        if( event == null )
            throw new NullPointerException();

        List<IEventHandler> listeners = listenerMap.get( event.type );
        if( listeners != null ) {
            for( IEventHandler el : listeners ) {
                el.handleEvent( event );
            }
        }
    }

    /**
     * Checks if listeners are installed for that event type.
     * @param type The event type.
     * @return True if a listener is installed, else false.
     */
    @Override
    public boolean hasEventListener( String type ) {
        // sanity check.
        if( type == null )
            throw new NullPointerException();
        return listenerMap.get( type ) != null;
    }
}
