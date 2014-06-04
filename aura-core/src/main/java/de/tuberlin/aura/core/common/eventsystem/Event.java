package de.tuberlin.aura.core.common.eventsystem;

import java.io.Serializable;

/**
 * The base class for all dispatched events.
 * 
 * @author Tobias Herb
 */
public class Event implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1L;

    public final String type;

    public final boolean sticky;

    private Object payload;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param type
     */
    public Event(final String type) {
        this(type, null, false);
    }

    /**
     * @param type
     * @param payload
     */
    public Event(final String type, Object payload) {
        this(type, payload, false);
    }

    public Event(final String type, Object payload, boolean sticky) {
        // sanity check.
        if (type == null)
            throw new IllegalArgumentException("type == null");

        this.type = type;
        setPayload(payload);
        this.sticky = sticky;
    }

    /**
     * @param payload
     */
    public void setPayload(final Object payload) {
        this.payload = payload;
    }

    /**
     * @return
     */
    public Object getPayload() {
        return this.payload;
    }

    /**
     * @return
     */
    @Override
    public String toString() {
        return type;
    }
}
