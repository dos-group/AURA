package de.tuberlin.aura.core.common.utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InetHelper
{
	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(InetHelper.class);

	public static InetAddress getIPAddress()
	{
		try
		{
			Enumeration<NetworkInterface> interfaces = NetworkInterface
				.getNetworkInterfaces();

			while (interfaces.hasMoreElements())
			{
				NetworkInterface curInterface = interfaces.nextElement();

				// Don't use the loopback interface.
				if (curInterface.isLoopback())
				{
					continue;
				}

				Enumeration<InetAddress> addresses = curInterface
					.getInetAddresses();
				while (addresses.hasMoreElements())
				{
					InetAddress curAddress = addresses.nextElement();

					// Check only for IPv4 addresses.
					if (curAddress instanceof Inet4Address)
					{
						return curAddress;
					}
				}
			}
		} catch (SocketException e)
		{
			LOG.error("I/O error", e);
		}

		return null;
	}
}