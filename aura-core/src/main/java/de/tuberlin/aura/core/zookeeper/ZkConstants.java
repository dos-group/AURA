package de.tuberlin.aura.core.zookeeper;

/**
 * This class contains all constant values that are used by multiple instances
 * in the aura application.
 * 
 * @author Christain Wuertz
 */
public class ZkConstants
{
	/**
	 * ZooKeeper session timeout in ms.
	 */
	public static final int ZOOKEEPER_TIMEOUT = 10000;

	/**
	 * Root folder in ZooKeeper.
	 */
	public static final String ZOOKEEPER_ROOT = "/aura";

	/**
	 * Folder for the task-managers.
	 */
	public static final String ZOOKEEPER_TASKMANAGERS = ZOOKEEPER_ROOT
			+ "/taskmanagers";
}
