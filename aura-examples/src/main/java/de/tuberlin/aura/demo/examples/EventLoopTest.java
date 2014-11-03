package de.tuberlin.aura.demo.examples;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.MultithreadEventExecutorGroup;

import java.util.*;
import java.util.concurrent.Callable;

/**
 *
 */
public class EventLoopTest {

    public static void main(String... args) {

        long t0 = System.currentTimeMillis();

        System.out.println("START " + t0);

        Random rand = new Random();

        List<Integer> intList = new ArrayList<>();

        for (int i = 0; i < 268435456; ++i) {
            intList.add(rand.nextInt(268435456));
        }

        Collections.sort(intList);

        long t1 = System.currentTimeMillis();

        System.out.println("STOP " + t1);

        System.out.println("ELAPSED TIME " + (((t1 - t0) / 1000) / 60));

        /*MultithreadEventExecutorGroup eventloop = new DefaultEventExecutorGroup(5);
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
        System.out.println(System.currentTimeMillis() + " main");*/

    }
}
