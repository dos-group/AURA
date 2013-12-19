package de.tuberlin.aura.core.iosystem;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents.ControlEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.ControlIOEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;


public final class IOManager extends EventDispatcher {

    //---------------------------------------------------
    // Inner Classes.
    //---------------------------------------------------

    private final class DataIOChannelHandler extends SimpleChannelInboundHandler<DataIOEvent> {

        @Override
        protected void channelRead0( final ChannelHandlerContext ctx, final DataIOEvent event )
                throws Exception {
            event.setChannel( ctx.channel() );
            dispatchEvent( event );
        }
    }

    private final class ControlIOChannelHandler extends SimpleChannelInboundHandler<ControlIOEvent> {

        @Override
        protected void channelRead0( final ChannelHandlerContext ctx, final ControlIOEvent event )
                throws Exception {
            event.setChannel( ctx.channel() );
            dispatchEvent( event );
        }
    }

    /**
     *
     */
    private final class ChannelBuilder {

        // All network output channels share the same event loop group.
        private final NioEventLoopGroup netOutputEventLoopGroup = new NioEventLoopGroup();

        public void buildNetworkDataChannel( final UUID srcTaskID, final UUID dstTaskID, final InetSocketAddress socketAddress ) {
            // sanity check.
            if( srcTaskID == null )
                throw new IllegalArgumentException( "srcTaskID == null" );
            if( dstTaskID == null )
                throw new IllegalArgumentException( "dstTaskID == null" );
            if( socketAddress == null )
                throw new IllegalArgumentException( "socketAddress == null" );

            final Bootstrap bootstrap = new Bootstrap();
            bootstrap.group( netOutputEventLoopGroup )
                     .channel( NioSocketChannel.class )
                     //.handler( new ObjectEncoder() );
                    .handler( new ChannelInitializer<SocketChannel>() {

                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addFirst( new ObjectEncoder() );
                            ch.pipeline().addFirst( new DataIOChannelHandler() );
                            ch.pipeline().addFirst( new ObjectDecoder( ClassResolvers.cacheDisabled( getClass().getClassLoader() ) ) );
                        }
                    } );


            final ChannelFuture cf = bootstrap.connect( socketAddress );
            cf.addListener( new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture cf)
                        throws Exception {
                    if( cf.isSuccess() ) {
                        cf.channel().writeAndFlush( new DataIOEvent( DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, srcTaskID, dstTaskID ) );
                        final DataIOEvent event = new DataIOEvent( DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, srcTaskID, dstTaskID );
                        event.setChannel( cf.channel() );
                        dispatchEvent( event );
                    } else {
                        LOG.error( "connection attempt failed: " + cf.cause().getLocalizedMessage() );
                    }
                }
            } );
        }

        public void buildLocalDataChannel( final UUID srcTaskID, final UUID dstTaskID ) {
            // sanity check.
            if( srcTaskID == null )
                throw new IllegalArgumentException( "srcTaskID == null" );
            if( dstTaskID == null )
                throw new IllegalArgumentException( "dstTaskID == null" );

            final Bootstrap bootstrap = new Bootstrap();
            bootstrap.group( netOutputEventLoopGroup )
                .channel( LocalChannel.class )
                .handler( new ChannelInitializer<LocalChannel>() {

                    @Override
                    public void initChannel(LocalChannel ch) throws Exception {
                        ch.pipeline().addFirst( new DataIOChannelHandler() );
                    }
                } );

            final ChannelFuture cf = bootstrap.connect( localAddress );
            cf.addListener( new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture cf)
                        throws Exception {
                    if( cf.isSuccess() ) {
                        cf.channel().writeAndFlush( new DataIOEvent( DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, srcTaskID, dstTaskID ) );
                        final DataIOEvent event = new DataIOEvent( DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED, srcTaskID, dstTaskID );
                        event.setChannel( cf.channel() );
                        dispatchEvent( event );
                    } else {
                        LOG.error( "connection attempt failed: " + cf.cause().getLocalizedMessage() );
                    }
                }
            } );
        }

        public void buildNetworkControlChannel( final UUID srcMachineID, final UUID dstMachineID, final InetSocketAddress socketAddress ) {
            // sanity check.
            if( socketAddress == null )
                throw new IllegalArgumentException( "socketAddress == null" );

            final Bootstrap bootstrap = new Bootstrap();
            bootstrap.group( netOutputEventLoopGroup )
                     .channel( NioSocketChannel.class )
                     .handler( new ChannelInitializer<SocketChannel>() {

                            @Override
                            public void initChannel(SocketChannel ch) throws Exception {
                                ch.pipeline().addFirst( new ObjectEncoder() );
                                ch.pipeline().addFirst( new ControlIOChannelHandler() );
                                ch.pipeline().addFirst( new ObjectDecoder( ClassResolvers.cacheDisabled( getClass().getClassLoader() ) ) );
                            }
                     } );

            final ChannelFuture cf = bootstrap.connect( socketAddress );
            cf.addListener( new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture cf)
                        throws Exception {

                    if( cf.isSuccess() ) {
                        cf.channel().writeAndFlush( new ControlIOEvent( ControlEventType.CONTROL_EVENT_INPUT_CHANNEL_CONNECTED, srcMachineID, dstMachineID ) );
                        final ControlIOEvent event = new ControlIOEvent( ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED, srcMachineID, dstMachineID );
                        event.setChannel( cf.channel() );
                        dispatchEvent( event );
                    } else {
                        LOG.error( "connection attempt failed: " + cf.cause().getLocalizedMessage() );
                    }
                }
            } );
        }
    }

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public IOManager( final MachineDescriptor machine ) {
        super( false );

        // sanity check.
        if( machine == null )
            throw new IllegalArgumentException( "machine == null" );

        this.machine = machine;

        channelBuilder = new ChannelBuilder();

        final NioEventLoopGroup nioInputEventLoopGroup = new NioEventLoopGroup();

        startNetworkDataMessageServer( this.machine, nioInputEventLoopGroup );

        startNetworkControlMessageServer( this.machine, nioInputEventLoopGroup );

        startLocalDataMessageServer( nioInputEventLoopGroup );
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    private static final Logger LOG = Logger.getLogger( IOManager.class );

    public final MachineDescriptor machine;

    private final ChannelBuilder channelBuilder;

    private final LocalAddress localAddress = new LocalAddress( "1" );

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

    public void connectDataChannel( final UUID srcTaskID, final UUID dstTaskID, final MachineDescriptor dstMachine ) {
        // sanity check.
        if( srcTaskID == null  )
            throw new IllegalArgumentException( "srcTask == null" );
        if( dstTaskID == null  )
            throw new IllegalArgumentException( "dstTask == null" );
        if( dstMachine == null  )
            throw new IllegalArgumentException( "dstTask == null" );

        if( machine.equals( dstMachine ) ) {
            channelBuilder.buildLocalDataChannel( srcTaskID, dstTaskID );
        } else {
            channelBuilder.buildNetworkDataChannel( srcTaskID, dstTaskID, dstMachine.dataAddress );
        }
    }

    public void connectMessageChannelBlocking( final UUID dstMachineID, final InetSocketAddress socketAddress ) {
        // sanity check.
        if( socketAddress == null )
            throw new IllegalArgumentException( "socketAddress == null" );
        if( machine.dataAddress.equals( socketAddress ) )
            throw new IllegalArgumentException( "can not setup message channel" );

        // TODO: brrrr, use something different to make the call blocking until the channel is established!
        // Maybe a design with a Future?

        final Lock threadLock = new ReentrantLock();
        final Condition condition = threadLock.newCondition();

        // TODO: that´s bullshit, we make here assumptions on the evaluation order of the event
        // handlers. Against the general event contract!
         final IEventHandler localHandler = new IEventHandler() {
                // runs in the channel thread...
                @Override
                public void handleEvent(Event e) {
                    if( e instanceof ControlIOEvent ) {
                        final ControlIOEvent event = (ControlIOEvent)e;
                        if( dstMachineID.equals( event.dstMachineID ) ) {
                            threadLock.lock();
                                condition.signal();
                            threadLock.unlock();
                        }
                    }
                }
            };

        addEventListener( ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED, localHandler );

        channelBuilder.buildNetworkControlChannel( machine.uid, dstMachineID, socketAddress );

        threadLock.lock();
        try {
            condition.await();
        } catch (InterruptedException e) {
            LOG.info( e );
        } finally {
            threadLock.unlock();
        }

        removeEventListener( ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED, localHandler );
    }

    //---------------------------------------------------
    // Private.
    //---------------------------------------------------

    private void startNetworkDataMessageServer( final MachineDescriptor machine, final NioEventLoopGroup nelg ) {
        final ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group( nelg )
                .channel( NioServerSocketChannel.class )
                .childHandler( new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch)
                            throws Exception {
                        ch.pipeline().addFirst( new ObjectEncoder() );
                        ch.pipeline().addFirst( new DataIOChannelHandler() );
                        ch.pipeline().addFirst( new ObjectDecoder( ClassResolvers.cacheDisabled( getClass().getClassLoader() ) ) );
                    }
                } );

        final ChannelFuture cf = bootstrap.bind( machine.dataAddress );
        cf.addListener( new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                if( cf.isSuccess() ) {
                    LOG.info( "network server bound to adress " + machine.dataAddress );
                } else {
                    LOG.error( "bound attempt failed: " + cf.cause().getLocalizedMessage() );
                    throw new IllegalStateException( "could not start netty network server" );
                }
            }
        } );

        // Wait until the netty-server is bound.
        try {
            cf.sync();
        } catch (InterruptedException e) {
            LOG.error( e );
        }
    }

    private void startLocalDataMessageServer( final NioEventLoopGroup nelg ) {

        final ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group( nelg )
            .channel( LocalServerChannel.class )
            .childHandler(new ChannelInitializer<LocalChannel>() {

                @Override
                protected void initChannel(LocalChannel ch)
                        throws Exception {
                    ch.pipeline().addFirst( new DataIOChannelHandler() );
                }
            });

        final ChannelFuture cf = bootstrap.bind( localAddress );
        cf.addListener( new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                if( cf.isSuccess() ) {
                    LOG.info( "local server bound to adress " + machine.dataAddress );
                } else {
                    LOG.error( "bound attempt failed: " + cf.cause().getLocalizedMessage() );
                    throw new IllegalStateException( "could not start netty local server" );
                }
            }
        } );

        // Wait until the netty-server is bound.
        try {
            cf.sync();
        } catch (InterruptedException e) {
            LOG.error( e );
        }
    }

    private void startNetworkControlMessageServer( final MachineDescriptor machine, final NioEventLoopGroup nelg ) {
        final ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group( nelg )
                .channel( NioServerSocketChannel.class )
                .childHandler( new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch)
                            throws Exception {
                        ch.pipeline().addFirst( new ObjectEncoder() );
                        ch.pipeline().addFirst( new ControlIOChannelHandler() );
                        ch.pipeline().addFirst( new ObjectDecoder( ClassResolvers.cacheDisabled( getClass().getClassLoader() ) ) );
                    }
                } );

        final ChannelFuture cf = bootstrap.bind( machine.controlAddress );
        cf.addListener( new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                if( cf.isSuccess() ) {
                    LOG.info( "network server bound to adress " + machine.dataAddress );
                } else {
                    LOG.error( "bound attempt failed: " + cf.cause().getLocalizedMessage() );
                    throw new IllegalStateException( "could not start netty network server" );
                }
            }
        } );

        // Wait until the netty-server is bound.
        try {
            cf.sync();
        } catch (InterruptedException e) {
            LOG.error( e );
        }
    }
}
