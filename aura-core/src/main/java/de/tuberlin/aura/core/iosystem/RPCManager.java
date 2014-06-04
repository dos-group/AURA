package de.tuberlin.aura.core.iosystem;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents.ControlEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.RPCCalleeResponseEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.RPCCallerRequestEvent;

public final class RPCManager {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    // for debugging use -1.
    private static final long RPC_RESPONSE_TIMEOUT = -1; // 5000; // in ms

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(RPCManager.class);

    private final IOManager ioManager;

    private final Map<Pair<Class<?>, UUID>, Object> cachedProxies;

    private final RPCEventHandler rpcEventHandler;

    private final ProtocolCalleeProxy calleeProxy;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param ioManager
     */
    public RPCManager(final IOManager ioManager) {
        // sanity check.
        if (ioManager == null)
            throw new IllegalArgumentException("ioManager == null");

        this.ioManager = ioManager;

        this.rpcEventHandler = new RPCEventHandler();

        this.cachedProxies = new HashMap<>();

        final String[] rpcEvents = {ControlEventType.CONTROL_EVENT_RPC_CALLER_REQUEST, ControlEventType.CONTROL_EVENT_RPC_CALLEE_RESPONSE};

        this.ioManager.addEventListener(rpcEvents, rpcEventHandler);

        this.calleeProxy = new ProtocolCalleeProxy();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * @param protocolImplementation
     * @param protocolInterface
     */
    public void registerRPCProtocolImpl(Object protocolImplementation, Class<?> protocolInterface) {
        // sanity check.
        if (protocolImplementation == null)
            throw new IllegalArgumentException("protocolImplementation == null");
        if (protocolInterface == null)
            throw new IllegalArgumentException("protocolInterface == null");

        // ProtocolCalleeProxy.registerProtocol(protocolImplementation, protocolInterface);
        calleeProxy.registerProtocol(protocolImplementation, protocolInterface);
    }

    /**
     * @param protocolInterface
     * @param dstMachine
     * @param <T>
     * @return
     */
    public <T> T getRPCProtocolProxy(final Class<T> protocolInterface, final MachineDescriptor dstMachine) {
        // sanity check.
        if (protocolInterface == null)
            throw new IllegalArgumentException("protocolInterface == null");
        if (dstMachine == null)
            throw new IllegalArgumentException("dstMachine == null");

        final Pair<Class<?>, UUID> proxyKey = new Pair<Class<?>, UUID>(protocolInterface, dstMachine.uid);
        @SuppressWarnings("unchecked")
        T proxy = (T) cachedProxies.get(proxyKey);
        if (proxy == null) {
            proxy = ProtocolCallerProxy.createProtocolProxy(dstMachine.uid, protocolInterface, ioManager);
            cachedProxies.put(proxyKey, proxy);
        }

        return proxy;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    /**
     *
     */
    public static final class MethodSignature implements Serializable {

        private static final long serialVersionUID = -1L;

        public final String className;

        public final String methodName;

        public final Class<?>[] argumentTypes;

        public final Object[] arguments;

        public final Class<?> returnType;

        public MethodSignature(String className, String methodName, Class<?>[] argumentTypes, Object[] arguments, Class<?> returnType) {
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
            } else {
                this.argumentTypes = null;
                this.arguments = null;
            }

            this.returnType = returnType;
        }
    }

    /**
     *
     */
    @SuppressWarnings("unused")
    private static final class ProtocolCallerProxy implements InvocationHandler {

        private final UUID dstMachineID;

        private final IOManager ioManager;

        private final static Map<UUID, CountDownLatch> callerTable = new HashMap<>();

        private final static Map<UUID, Object> callerResultTable = new HashMap<>();

        public ProtocolCallerProxy(final UUID dstMachineID, final IOManager ioManager) {
            // sanity check.
            if (ioManager == null)
                throw new IllegalArgumentException("ioManager == null");

            this.dstMachineID = dstMachineID;

            this.ioManager = ioManager;
        }

        @SuppressWarnings("unchecked")
        public static <T> T createProtocolProxy(final UUID dstMachineID, final Class<T> protocolInterface, final IOManager ioManager) {

            final ProtocolCallerProxy pc = new ProtocolCallerProxy(dstMachineID, ioManager);
            return (T) Proxy.newProxyInstance(protocolInterface.getClassLoader(), new Class[] {protocolInterface}, pc);
        }

        /**
         * TODO: It should be possible to invoke methods in a non-blocking fashion. This could speed
         * up our deployment time significantly!
         * 
         * @param proxy
         * @param method
         * @param methodArguments
         * @return
         * @throws Throwable
         */
        @Override
        public Object invoke(Object proxy, Method method, Object[] methodArguments) throws Throwable {

            // check if all arguments implement serializable
            int argumentIndex = 0;
            for (final Object argument : methodArguments) {
                if (!(argument instanceof Serializable))
                    throw new IllegalStateException("argument [" + argumentIndex + "] is not instance of" + "<"
                            + Serializable.class.getCanonicalName() + ">");
                ++argumentIndex;
            }

            final MethodSignature methodInfo =
                    new MethodSignature(method.getDeclaringClass().getSimpleName(),
                                        method.getName(),
                                        method.getParameterTypes(),
                                        methodArguments,
                                        method.getReturnType());

            // every remote call is identified by a unique id. The id is used to
            // resolve the associated response from remote site.
            final UUID callUID = UUID.randomUUID();
            final CountDownLatch cdl = new CountDownLatch(1);
            callerTable.put(callUID, cdl);

            // send to server...
            ioManager.sendEvent(dstMachineID, new RPCCallerRequestEvent(callUID, methodInfo));

            try {
                if (RPC_RESPONSE_TIMEOUT > 0) {
                    // block the caller thread until we get some response...
                    // ...but with a specified timeout to avoid indefinitely blocking of caller.
                    cdl.await(RPC_RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
                } else {
                    cdl.await();
                }
            } catch (InterruptedException e) {
                LOG.info(e.getLocalizedMessage(), e);
            }

            // if is no result hasFree, then a response time-out happened...
            if (!callerResultTable.containsKey(callUID))
                throw new IllegalStateException("no result of remote call " + callUID + " hasFree");

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
            // final CountDownLatch cdl = RPCManager.ProtocolCallerProxy.callerTable.get(callUID);
            final CountDownLatch cdl = callerTable.get(callUID);
            cdl.countDown();
        }
    }

    /**
     *
     */
    private final class ProtocolCalleeProxy {

        private final Map<String, Object> calleeTable = new HashMap<>();

        public void registerProtocol(final Object protocolImplementation, final Class<?> protocolInterface) {
            calleeTable.put(protocolInterface.getSimpleName(), protocolImplementation);
        }

        public RPCCalleeResponseEvent callMethod(final UUID callUID, final MethodSignature methodInfo) {
            // sanity check.
            if (callUID == null)
                throw new IllegalArgumentException("callUID == null");
            if (methodInfo == null)
                throw new IllegalArgumentException("methodInfo == null");

            final Object protocolImplementation = calleeTable.get(methodInfo.className);

            if (protocolImplementation == null) {
                return new RPCCalleeResponseEvent(callUID, new IllegalStateException("found no protocol implementation"));
            }

            synchronized (protocolImplementation) { // TODO: Synchronization on local variable
                                                    // 'protocolImplementation'
                // TODO: Maybe we could do some caching of method signatures
                // on the callee site for frequent repeated calls...

                try {
                    final Method method = protocolImplementation.getClass().getMethod(methodInfo.methodName, methodInfo.argumentTypes);
                    final Object result = method.invoke(protocolImplementation, methodInfo.arguments);
                    return new RPCCalleeResponseEvent(callUID, result);
                } catch (Exception e) {
                    return new RPCCalleeResponseEvent(callUID, e);
                }
            }
        }
    }

    /**
     *
     */
    private final class RPCEventHandler extends EventHandler {

        private ExecutorService executor = Executors.newCachedThreadPool();

        @Handle(event = RPCCallerRequestEvent.class)
        private void handleRPCRequest(final RPCCallerRequestEvent event) {
            this.executor.execute(new Runnable() {

                @Override
                public void run() {
                    final RPCCalleeResponseEvent calleeMsg =
                    // RPCManager.ProtocolCalleeProxy.callMethod(event.callUID,
                    // event.methodSignature);
                            calleeProxy.callMethod(event.callUID, event.methodSignature);

                    ioManager.sendEvent(event.getSrcMachineID(), calleeMsg);
                }
            });
        }

        @Handle(event = RPCCalleeResponseEvent.class)
        private void handleRPCResponse(final RPCCalleeResponseEvent event) {
            ProtocolCallerProxy.notifyCaller(event.callUID, event.result);
        }
    }
}
