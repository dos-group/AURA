package de.tuberlin.aura.core.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * TODO: Put all watchers in one class like the descriptors?
 */
public class ZkConnectionWatcher implements Watcher
{
	/**
	 * Logger.
	 */
	private static final Logger LOG = Logger
			.getLogger(ZkConnectionWatcher.class);

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void process(WatchedEvent event)
	{
		LOG.debug("Received event: " + event.getState().toString());

		// Check the state of the received event.
		switch (event.getState())
		{
			case Expired:
				// TODO: Inform the holder of the connection about this event.

				// // It's all over
				// try
				// {
				//
				// this.zk.close();
				// }
				// catch (InterruptedException e)
				// {
				// LOG.error(
				// "Couldn't shut down the ZooKeeper connection properly.", e);
				// }
				break;
			default:
				// Nothing special to do
				break;
		}
	}
}
