package de.tuberlin.aura.core.common.eventsystem;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.Logger;

public abstract class EventHandler implements IEventHandler {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    @Retention(RetentionPolicy.RUNTIME)
    public static @interface Handle {

        Class<? extends Event> event();

        String type() default "";
    }

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public EventHandler() {

        this.eventHandlerMap = new HashMap<Class<?>, Method>();

        this.multiTypeEventHandlerMap = new HashMap<Class<?>, Map<String, Method>>();

        register(this.getClass());
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(EventHandler.class);

    private final Map<Class<?>, Method> eventHandlerMap;

    private final Map<Class<?>, Map<String, Method>> multiTypeEventHandlerMap;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    @Override
    public void handleEvent(final Event event) {
        Method m = eventHandlerMap.get(event.getClass());
        if (m != null) {
            m.setAccessible(true);
            try {
                m.invoke(this, event);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        } else {
            final Map<String, Method> handlerTable = multiTypeEventHandlerMap.get(event.getClass());

            m = (handlerTable != null) ? handlerTable.get(event.type) : null;
            if (m != null) {
                m.setAccessible(true);
                try {
                    m.invoke(this, event);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new IllegalStateException(e);
                }
            } else {
                handleUnknownEvent(event);
            }
        }
    }

    // ---------------------------------------------------
    // Protected.
    // ---------------------------------------------------

    protected void handleUnknownEvent(final Event e) {
        // throw new IllegalStateException( e.toString() );
        LOG.debug("unknown event " + e);
    }

    // ---------------------------------------------------
    // Private.
    // ---------------------------------------------------

    private void register(final Class<?> clazz) {
        for (final Method m : clazz.getDeclaredMethods()) {
            if (m.isAnnotationPresent(Handle.class)) {
                final Class<?>[] argumentTypes = m.getParameterTypes();
                if (argumentTypes.length == 1 && Event.class.isAssignableFrom(argumentTypes[0])) {
                    final Handle handle = m.getAnnotation(Handle.class);
                    if ("".equals(handle.type())) {

                        if (eventHandlerMap.containsKey(handle.type()))
                            throw new IllegalStateException("event already registered");

                        eventHandlerMap.put(handle.event(), m);

                    } else {
                        Map<String, Method> handlerTable = multiTypeEventHandlerMap.get(handle.event());
                        if (handlerTable == null) {
                            handlerTable = new Hashtable<String, Method>();
                            multiTypeEventHandlerMap.put(handle.event(), handlerTable);
                        }

                        if (handlerTable.containsKey(handle.type()))
                            throw new IllegalStateException("event already registered");

                        handlerTable.put(handle.type(), m);
                    }
                } else {
                    throw new IllegalStateException(m + " is not a event handler method");
                }
            }
        }
    }
}
