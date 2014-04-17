package de.tuberlin.aura.core.zookeeper;

import net.jcip.annotations.ThreadSafe;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.IEventHandler;

/**
 * TODO: Put all watchers in one class like the descriptors?
 */
@ThreadSafe
public class ZookeeperConnectionWatcher implements Watcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperConnectionWatcher.class);

    /**
     * Events received by this class are passed on to this handler.
     */
    private IEventHandler handler;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * Constructor.
     * 
     * @param handler Forward all events that are received by this class to this event handler for
     *        further processing.
     */
    public ZookeeperConnectionWatcher(IEventHandler handler) {
        this.handler = handler;
    }

    // ---------------------------------------------------
    // Public Methods.
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
                LOG.error("Session expired");
                de.tuberlin.aura.core.common.eventsystem.Event zkEvent =
                        new de.tuberlin.aura.core.common.eventsystem.Event(ZookeeperHelper.EVENT_TYPE_CONNECTION_EXPIRED);
                this.handler.handleEvent(zkEvent);
                break;
            default:
                // Nothing special to do
                break;
        }
    }
}
