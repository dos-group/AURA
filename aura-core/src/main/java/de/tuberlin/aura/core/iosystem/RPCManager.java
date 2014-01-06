package de.tuberlin.aura.core.iosystem;

import io.netty.channel.Channel;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;

public final class RPCManager {

	// ---------------------------------------------------
	// Constants.
	// ---------------------------------------------------

	// for debugging use -1.
	private static final long RPC_RESPONSE_TIMEOUT = -1; // 5000; // in ms

	// ---------------------------------------------------
	// Inner Classes.
	// ---------------------------------------------------

	/**
     *
     */
	public static final class MethodSignature implements Serializable {

		private static final long serialVersionUID = 698401142453803590L;

		public MethodSignature(String className,
				String methodName,
				Class<?>[] argumentTypes,
				Object[] arguments,
				Class<?> returnType) {
			// sanity check.
			if (className == null)
				throw new IllegalArgumentException("className == null");
			if (methodName == null)
				throw new IllegalArgumentException("methodName == null");
			if (returnType == null)
				throw new IllegalArgumentException("methodArguments == null");

			this.className = className;

			this.methodName = methodName;

			if (arguments != null) {
				this.argumentTypes = argumentTypes;
				this.arguments = arguments;
				// this.arguments = new Object[arguments.length];
				// System.arraycopy( arguments, 0, this.arguments, 0, arguments.length );
			} else {
				this.argumentTypes = null;
				this.arguments = null;
			}

			this.returnType = returnType;
		}

		public final String className;

		public final String methodName;

		public final Class<?>[] argumentTypes;

		public final Object[] arguments;

		public final Class<?> returnType;
	}

	/**
     *
     */
	public static final class RPCCallerMessage implements Serializable {

		private static final long serialVersionUID = -6373851435734281315L;

		public RPCCallerMessage(final UUID callUID, final MethodSignature methodSignature) {
			// sanity check.
			if (callUID == null)
				throw new IllegalArgumentException("callUID == null");
			if (methodSignature == null)
				throw new IllegalArgumentException("methodSignature == null");

			this.callUID = callUID;

			this.methodSignature = methodSignature;
		}

		public UUID callUID;

		public MethodSignature methodSignature;
	}

	/**
     *
     */
	public static final class RPCCalleeMessage implements Serializable {

		private static final long serialVersionUID = 7730876178491490347L;

		public RPCCalleeMessage(final UUID callUID, final Object result) {
			// sanity check.
			if (callUID == null)
				throw new IllegalArgumentException("callUID == null");

			this.callUID = callUID;

			this.result = result;
		}

		public UUID callUID;

		public Object result;
	}

	/**
     *
     */
	@SuppressWarnings("unused")
	public static final class ProtocolCallerProxy implements InvocationHandler {

		public ProtocolCallerProxy(Channel channel) {
			// sanity check.
			if (channel == null)
				throw new IllegalArgumentException("channel == null");

			this.channel = channel;
		}

		private final Channel channel;

		private static final Map<UUID, CountDownLatch> callerTable = new HashMap<UUID, CountDownLatch>();

		private static final Map<UUID, Object> callerResultTable = new HashMap<UUID, Object>();

		@SuppressWarnings("unchecked")
		public static <T> T getProtocolProxy(final Class<T> protocolInterface, final Channel channel) {
			final ProtocolCallerProxy pc = new ProtocolCallerProxy(channel);
			return (T) Proxy
				.newProxyInstance(protocolInterface.getClassLoader(), new Class[] { protocolInterface }, pc);
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] methodArguments)
				throws Throwable {

			// check if all arguments implement serializable
			int argumentIndex = 0;
			for (final Object argument : methodArguments) {
				if (!(argument instanceof Serializable))
					throw new IllegalStateException("argument [" + argumentIndex + "] is not instance of" +
						"<" + Serializable.class.getCanonicalName() + ">");
				++argumentIndex;
			}

			final MethodSignature methodInfo = new MethodSignature(
				method.getDeclaringClass().getSimpleName(),
				method.getName(),
				method.getParameterTypes(),
				methodArguments,
				method.getReturnType()
				);

			// every remote call is identified by a unique id. The id is used to
			// resolve the associated response from remote site.
			final UUID callUID = UUID.randomUUID();
			final CountDownLatch cdl = new CountDownLatch(1);
			callerTable.put(callUID, cdl);

			// send to server...
			channel.writeAndFlush(new RPCCallerMessage(callUID, methodInfo));

			try {
				if (RPC_RESPONSE_TIMEOUT > 0) {
					// block the caller thread until we get some response...
					// ...but with a specified timeout to avoid indefinitely blocking of caller.
					cdl.await(RPC_RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
				} else {
					cdl.await();
				}
			} catch (InterruptedException e) {
				LOG.info(e);
			}

			// if is no result available, then a response time-out happened...
			if (!callerResultTable.containsKey(callUID))
				throw new IllegalStateException("no result of remote call " + callUID + " available");

			// result is allowed to be null -> void as return type.
			final Object result = callerResultTable.get(callUID);
			// clean up our tables.
			callerResultTable.remove(callUID);
			callerTable.remove(callUID);

			// TODO: should we pass a crashed call to the caller?
			if (result instanceof Throwable)
				throw new IllegalStateException((Throwable) result);

			return result;
		}

		public static void notifyCaller(final UUID callUID, final Object result) {
			// sanity check.
			if (callUID == null)
				throw new IllegalArgumentException("callUID == null");

			callerResultTable.put(callUID, result);
			final CountDownLatch cdl = RPCManager.ProtocolCallerProxy.callerTable.get(callUID);
			cdl.countDown();
		}
	}

	/**
     *
     */
	public static final class ProtocolCalleeProxy {

		private static final Map<String, Object> calleeTable = new HashMap<String, Object>();

		public static void registerProtocol(final Object protocolImplementation, final Class<?> protocolInterface) {
			calleeTable.put(protocolInterface.getSimpleName(), protocolImplementation);
		}

		public static RPCCalleeMessage callMethod(final UUID callUID, final MethodSignature methodInfo) {
			// sanity check.
			if (callUID == null)
				throw new IllegalArgumentException("callUID == null");
			if (methodInfo == null)
				throw new IllegalArgumentException("methodInfo == null");

			final Object protocolImplementation = calleeTable.get(methodInfo.className);

			if (protocolImplementation == null) {
				return new RPCCalleeMessage(callUID, new IllegalStateException("found no protocol implementation"));
			}

			synchronized (protocolImplementation) {
				// TODO: Maybe we could do some caching of method signatures
				// on the callee site for frequent repeated calls...

				try {
					final Method method = protocolImplementation.getClass().getMethod(methodInfo.methodName,
						methodInfo.argumentTypes);
					final Object result = method.invoke(protocolImplementation, methodInfo.arguments);
					return new RPCCalleeMessage(callUID, result);
				} catch (Exception e) {
					return new RPCCalleeMessage(callUID, e);
				}
			}
		}
	}

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public RPCManager(final IOManager ioManager) {
		// sanity check.
		if (ioManager == null)
			throw new IllegalArgumentException("ioManager == null");

		this.ioManager = ioManager;

		this.rpcChannelMap = new ConcurrentHashMap<UUID, Channel>();

		this.ioManager.addEventListener(IOEvents.IOControlChannelEvent.IO_EVENT_MESSAGE_CHANNEL_CONNECTED,
			new IEventHandler() {

				@Override
				public void handleEvent(Event e) {
					if (e instanceof IOEvents.IOControlChannelEvent) {
						final IOEvents.IOControlChannelEvent event = (IOEvents.IOControlChannelEvent) e;
						if (event.isIncoming)
							rpcChannelMap.put(event.srcMachineID, event.controlChannel);
						else
							rpcChannelMap.put(event.dstMachineID, event.controlChannel);
						LOG.info("message connection between machine " + event.srcMachineID
							+ " and " + event.dstMachineID + " established");
					}
				}
			});

		this.cachedProxies = new HashMap<Pair<Class<?>, UUID>, Object>();
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	private static final Logger LOG = Logger.getLogger(RPCManager.class);

	private final IOManager ioManager;

	private final Map<UUID, Channel> rpcChannelMap;

	private final Map<Pair<Class<?>, UUID>, Object> cachedProxies;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	public void connectToMessageServer(final MachineDescriptor dstMachine) {
		// sanity check.
		if (dstMachine == null)
			throw new IllegalArgumentException("machine == null");

		ioManager.connectMessageChannelBlocking(dstMachine.uid, dstMachine.controlAddress);
	}

	public void registerRPCProtocolImpl(Object protocolImplementation, Class<?> protocolInterface) {
		// sanity check.
		if (protocolImplementation == null)
			throw new IllegalArgumentException("protocolImplementation == null");
		if (protocolInterface == null)
			throw new IllegalArgumentException("protocolInterface == null");

		ProtocolCalleeProxy.registerProtocol(protocolImplementation, protocolInterface);
	}

	public <T> T getRPCProtocolProxy(final Class<T> protocolInterface, final MachineDescriptor dstMachine) {
		// sanity check.
		if (protocolInterface == null)
			throw new IllegalArgumentException("protocolInterface == null");
		if (dstMachine == null)
			throw new IllegalArgumentException("dstMachine == null");

		final Channel channel = rpcChannelMap.get(dstMachine.uid);
		if (channel == null)
			throw new IllegalStateException("channel == null");

		final Pair<Class<?>, UUID> proxyKey = new Pair<Class<?>, UUID>(protocolInterface, dstMachine.uid);
		@SuppressWarnings("unchecked")
		T proxy = (T) cachedProxies.get(proxyKey);
		if (proxy == null) {
			proxy = ProtocolCallerProxy.getProtocolProxy(protocolInterface, channel);
			cachedProxies.put(proxyKey, proxy);
		}
		return proxy;
	}

	public Channel getChannel(final MachineDescriptor machine) {
		// sanity check.
		if (machine == null)
			throw new IllegalArgumentException("machine == null");

		return rpcChannelMap.get(machine.uid);
	}
}
