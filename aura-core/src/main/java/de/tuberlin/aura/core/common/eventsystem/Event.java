package de.tuberlin.aura.core.common.eventsystem;

import java.io.Serializable;

/**
 * The base class for all dispatched events.
 *
 * @author Tobias Herb
 */
public class Event implements Serializable {

	private static final long serialVersionUID = 1L;

	public Event(final String type) {
		// sanity check.
		if (type == null)
			throw new IllegalArgumentException("type == null");

		this.type = type;
	}

	public Event(final String type, Object payload) {
		// sanity check.
		if (type == null)
			throw new IllegalArgumentException("type == null");

		this.type = type;
		setPayload(payload);
	}


	public final String type;

	private Object payload;

	public void setPayload(final Object payload) {
		this.payload = payload;
	}

	public Object getPayload() {
		return this.payload;
	}

	@Override
	public String toString() {
		return type;
	}
}
