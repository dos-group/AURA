package de.tuberlin.aura.core.zookeeper;

import net.jcip.annotations.ThreadSafe;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import de.tuberlin.aura.core.common.eventsystem.IEventHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: Put all watchers in one class like the descriptors?
 */
@ThreadSafe
public class ZkConnectionWatcher implements Watcher {

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	/** Logger.*/
	private static final Logger LOG = LoggerFactory
		.getLogger(ZkConnectionWatcher.class);

	/** Events received by this class are passed on to this handler.*/
	private IEventHandler handler;

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	/**
	 * Constructor.
	 *
	 * @param handler
	 *        Forward all events that are received by this class to this
	 *        event handler for further processing.
	 */
	public ZkConnectionWatcher(IEventHandler handler) {
		this.handler = handler;
	}

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void process(WatchedEvent event) {
		LOG.debug("Received event: {}", event.getState().toString());

		// Check the state of the received event.
		switch (event.getState()) {
		case Expired:
			de.tuberlin.aura.core.common.eventsystem.Event zkEvent = new de.tuberlin.aura.core.common.eventsystem.Event(
				ZkHelper.EVENT_TYPE_CONNECTION_EXPIRED);
			this.handler.handleEvent(zkEvent);
			break;
		default:
			// Nothing special to do
			break;
		}
	}
}