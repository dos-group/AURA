package de.tuberlin.aura.core.common.eventsystem;

/**
 * The std class for all dispatched events.
 * 
 * @author Tobias Herb
 */
public class Event {

	/**
	 * Constructor.
	 */
	public Event(String type) {
		this(type, null);
	}

	/**
	 * Constructor.
	 */
	public Event(String type, Object data) {
		// sanity check.
		if (type == null)
			throw new NullPointerException();

		this.type = type;
		this.data = data;
	}

	/** Defines the type of the event. */
	public final String type;

	/** The event user data. */
	public final Object data;
}
