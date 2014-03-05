package de.tuberlin.aura.core.common.eventsystem;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

/**
 * The EventDispatcher is responsible for adding and removing listeners and for dispatching event to
 * this listeners.
 * 
 * @author Tobias Herb
 */
public class EventDispatcher implements IEventDispatcher {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public EventDispatcher() {
        this(false);
    }

    public EventDispatcher(boolean useDispatchThread) {

        this.listenerMap = new ConcurrentHashMap<String, List<IEventHandler>>();

        this.useDispatchThread = useDispatchThread;

        this.isRunning = new AtomicBoolean(useDispatchThread);

        this.eventQueue = useDispatchThread ? new LinkedBlockingQueue<Event>() : null;

        this.dispatcherThread = useDispatchThread ? new Runnable() {

            @Override
            public void run() {
                while (isRunning.get()) {
                    try {
                        final Event event = eventQueue.take();
                        dispatch(event);
                    } catch (InterruptedException e) {
                        LOG.error(e);
                    }
                }
            }
        } : null;

        if (useDispatchThread) {
            new Thread(dispatcherThread).start();
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(EventDispatcher.class);

    private final Map<String, List<IEventHandler>> listenerMap;

    private final boolean useDispatchThread;

    private final Runnable dispatcherThread;

    private final AtomicBoolean isRunning;

    private final BlockingQueue<Event> eventQueue;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    /**
     * Add a listener for a specific event.
     * 
     * @param type The event type.
     * @param listener The handler for this event.
     */
    @Override
    public synchronized void addEventListener(final String type, final IEventHandler listener) {
        // sanity check.
        if (type == null)
            throw new IllegalArgumentException("type == null");
        if (listener == null)
            throw new IllegalArgumentException("listener == null");

        List<IEventHandler> listeners = listenerMap.get(type);
        if (listeners == null) {
            listeners = new LinkedList<IEventHandler>();
            listenerMap.put(type, listeners);
        }
        listeners.add(listener);
    }

    @Override
    public void addEventListener(final String[] types, final IEventHandler listener) {
        // sanity check.
        if (types == null)
            throw new IllegalArgumentException("types == null");
        if (listener == null)
            throw new IllegalArgumentException("listener == null");

        for (final String type : types)
            addEventListener(type, listener);
    }

    /**
     * Remove a listener for a specific event.
     * 
     * @param type The event type.
     * @param listener The handler for this event.
     */
    @Override
    public synchronized boolean removeEventListener(final String type, final IEventHandler listener) {
        // sanity check.
        if (type == null)
            throw new IllegalArgumentException("type == null");
        if (listener == null)
            throw new IllegalArgumentException("listener == null");

        final List<IEventHandler> listeners = listenerMap.get(type);
        if (listeners != null) {
            boolean isRemoved = listeners.remove(listener);
            // if no more listeners registered, we remove the complete mapping.
            if (isRemoved && listeners.size() == 0) {
                listenerMap.remove(type);
            }
            return isRemoved;
        } else {
            return false;
        }
    }

    @Override
    public synchronized void removeAllEventListener() {
        for (final List<IEventHandler> listeners : listenerMap.values()) {
            listeners.clear();
        }
        listenerMap.clear();
    }

    /**
     * Dispatch a event.
     * 
     * @param event The event to dispatch.
     */
    @Override
    public void dispatchEvent(final Event event) {
        // sanity check.
        if (event == null)
            throw new IllegalArgumentException("event == null");

        if (useDispatchThread) {
            eventQueue.add(event);
        } else {
            dispatch(event);
        }
    }

    /**
     * Checks if listeners are installed for that event type.
     * 
     * @param type The event type.
     * @return True if a listener is installed, else false.
     */
    @Override
    public boolean hasEventListener(final String type) {
        // sanity check.
        if (type == null)
            throw new IllegalArgumentException("type == null");

        return listenerMap.get(type) != null;
    }

    public void shutdownEventQueue() {
        isRunning.set(false);
    }

    // ---------------------------------------------------
    // Private.
    // ---------------------------------------------------

    private synchronized void dispatch(final Event event) {
        final List<IEventHandler> listeners = listenerMap.get(event.type);
        if (listeners != null) {
            for (final IEventHandler el : listeners) {
                el.handleEvent(event);
            }
        } else { // listeners == null
            LOG.info("no listener registered for event " + event.type);
        }
    }
}
