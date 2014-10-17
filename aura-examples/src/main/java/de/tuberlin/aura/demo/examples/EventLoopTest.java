package de.tuberlin.aura.demo.examples;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.MultithreadEventExecutorGroup;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 *
 */
public class EventLoopTest {

    public static void main(String... args) {
        MultithreadEventExecutorGroup eventloop = new DefaultEventExecutorGroup(5);
        Future<Void> f = eventloop.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                int wait = 5000;
                try {
                    Thread.sleep(wait);
                } catch (Exception e) {}
                System.out.println(System.currentTimeMillis() + " Thread");
                return null;
            }
        });
        f.addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                if (future.isSuccess()) {
                    System.out.println(System.currentTimeMillis() + " success");
                }
            }
        });
        System.out.println(System.currentTimeMillis() + " main");

    }
}
