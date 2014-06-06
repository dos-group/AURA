package de.tuberlin.aura.core.common.eventsystem;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.iosystem.IOEvents;

/**
 * The EventDispatcher is responsible for adding and removing listeners and for dispatching event to
 * this listeners.
 * 
 * @author Tobias Herb
 */
public class EventDispatcher implements IEventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(EventDispatcher.class);

    private final Map<String, List<IEventHandler>> listenerMap;

    private final boolean useDispatchThread;

    private final Thread dispatcherThread;

    private final AtomicBoolean isRunning;

    private final BlockingQueue<Event> eventQueue;

    private final List<Event> stickyEvents;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     *
     */
    public EventDispatcher() {
        this(false, null);
    }

    /**
     *
     */
    public EventDispatcher(boolean useDispatchThread) {
        this(useDispatchThread, null);
    }

    /**
     * @param useDispatchThread
     */
    public EventDispatcher(boolean useDispatchThread, String name) {

        this.listenerMap = new ConcurrentHashMap<>();

        this.useDispatchThread = useDispatchThread;

        this.isRunning = new AtomicBoolean(useDispatchThread);

        this.stickyEvents = new LinkedList<>();

        this.eventQueue = useDispatchThread ? new LinkedBlockingQueue<Event>() : null;

        if (useDispatchThread) {
            Runnable runnable = new Runnable() {

                @Override
                public void run() {
                    while (isRunning.get() || eventQueue.size() != 0) {
                        try {
                            final Event event = eventQueue.take();
                            LOG.trace("Process event {} - events left in queue: {}", event.type, eventQueue.size());

                            // Stop dispatching thread, if a poison pill was received.
                            if (event.type != IOEvents.InternalEventType.POISON_PILL_TERMINATION) {
                                if (!dispatch(event)) {
                                    if (event.sticky) {
                                        LOG.debug("Add sticky event {}", event);
                                        EventDispatcher.this.stickyEvents.add(event);
                                    }
                                }
                            } else {
                                LOG.trace("Poison pill received");
                            }
                        } catch (InterruptedException e) {
                            LOG.error(e.getLocalizedMessage(), e);
                        }
                    }

                    LOG.trace("Event dispatcher thread stopped.");
                }
            };

            if (name != null) {
                this.dispatcherThread = new Thread(runnable, name);
            } else {
                this.dispatcherThread = new Thread(runnable);
            }

            this.dispatcherThread.start();
        } else {
            this.dispatcherThread = null;
        }
    }

    // ---------------------------------------------------
    // Public Methods.
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
            listeners = new LinkedList<>();
            listenerMap.put(type, listeners);
        }

        listeners.add(listener);
        checkStickyEvents(type, listener);
    }

    /**
     * @param types
     * @param listener
     */
    @Override
    public synchronized void addEventListener(final String[] types, final IEventHandler listener) {
        // sanity check.
        if (types == null)
            throw new IllegalArgumentException("types == null");
        if (listener == null)
            throw new IllegalArgumentException("listener == null");

        for (final String type : types) {
            addEventListener(type, listener);
            checkStickyEvents(type, listener);
        }
    }

    private void checkStickyEvents(final String type, final IEventHandler listener) {
        Iterator<Event> it = this.stickyEvents.iterator();
        while (it.hasNext()) {
            Event event = it.next();

            if (event.type.equals(type)) {
                LOG.info("Process sticky event {}", event);

                if (useDispatchThread) {
                    eventQueue.add(event);
                } else {
                    listener.handleEvent(event);
                }

                // This event was handled now and thus it can be removed.
                it.remove();
            }
        }
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

    /**
     *
     */
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

            // Use this place for debugging if you not know where
            // in the code the event gets dispatched.

            // if(event.type.equals("CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE")) {
            // LOG.info("");
            // }

        } else {
            if (!dispatch(event)) {
                if (event.sticky) {
                    LOG.info("Add sticky event {}", event);
                    EventDispatcher.this.stickyEvents.add(event);
                }
            }
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

    /**
     * TODO: Can be remove -> see shutdown method
     */
    @Deprecated
    public void shutdownEventQueue() {
        isRunning.set(false);
    }

    public void setName(String name) {
        if (useDispatchThread) {
            this.dispatcherThread.setName(name);
        }
    }

    @Override
    public void shutdown() {
        if (useDispatchThread) {
            LOG.trace("Shutdown event dispatcher");

            isRunning.set(false);

            // Feed the poison pill to the event dispatcher thread to terminate it.
            eventQueue.add(new Event(IOEvents.InternalEventType.POISON_PILL_TERMINATION));
        }
    }

    /**
     *
     * @return
     */
    @Override
    public void joinDispatcherThread() {
        if(useDispatchThread) {
            try {
                dispatcherThread.join();
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage());
            }
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     * @param event
     */
    private synchronized boolean dispatch(final Event event) {
        final List<IEventHandler> listeners = listenerMap.get(event.type);
        if (listeners != null) {
            for (final IEventHandler el : listeners) {
                try {
                    el.handleEvent(event);
                } catch (Throwable t) {
                    LOG.error(t.getLocalizedMessage(), t);
                    throw t;
                }
            }
        } else { // listeners == null
            LOG.info("no listener registered for event " + event.type);

            if (event instanceof IOEvents.DataIOEvent) {
                IOEvents.DataIOEvent e2 = (IOEvents.DataIOEvent) event;
                LOG.info("type {} from {} to {}", e2.type, e2.srcTaskID, e2.dstTaskID);
            }

            // Event wasn't processed by any event handler.
            return false;
        }

        return true;
    }
}
