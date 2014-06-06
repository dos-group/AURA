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

    public  String type;

    private Object payload;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     *
     */
    public Event() {

    }

    /**
     * @param type
     */
    public Event(final String type) {
        // sanity check.
        if (type == null)
            throw new IllegalArgumentException("type == null");

        this.type = type;
    }

    /**
     * @param type
     * @param payload
     */
    public Event(final String type, Object payload) {
        // sanity check.
        if (type == null)
            throw new IllegalArgumentException("type == null");

        this.type = type;
        setPayload(payload);
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
