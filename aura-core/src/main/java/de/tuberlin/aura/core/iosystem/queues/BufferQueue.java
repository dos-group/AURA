package de.tuberlin.aura.core.iosystem.queues;

import java.util.concurrent.TimeUnit;

public interface BufferQueue<T> {

    /**
     * Tries to retrieve the head of the queue and blocks for the specified time period if no
     * element is available.
     * 
     * @param timeout the time to wait
     * @param timeUnit the time unit
     * @return the head of the queue, or null if no element is available
     * @throws InterruptedException if the method is interrupted during wait.
     */
    T poll(final long timeout, final TimeUnit timeUnit) throws InterruptedException;

    /**
     * Tries to retrieve the head of the list if available.
     * 
     * @return the head of the queue, or null if no element is available
     */
    T poll();

    /**
     * Retrieves and removes the head of this queue, waiting if necessary until an element becomes
     * available.
     * 
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    T take() throws InterruptedException;

    /**
     * Inserts the specified element at the tail of this queue, waiting if necessary for space to
     * become available.
     * 
     * @param value the element to add
     * @throws InterruptedException if interrupted while waiting
     */
    void put(T value) throws InterruptedException;

    /**
     * Tries to insert the specified element into the queue if possible.
     * 
     * @param value the element to add
     * @return true if the element is added, false otherwise
     */
    boolean offer(T value);

    /**
     * Returns true if the queue contains no elements.
     * 
     * @return true if the queue contains no elements, false otherwise
     */
    boolean isEmpty();

    /**
     * Returns the current size of the queue.
     * 
     * @return the size of the queue
     */
    int size();

    // factory

    /**
     * Factory for queue instantiation.
     * 
     * @param <T> the type of the elements of the queue
     */
    public interface FACTORY<T> {

        /**
         * Returns a new instance of the specific queue.
         * 
         * @return a new instance of the specific queue.
         */
        BufferQueue<T> newInstance();
    }

    // observer

    /**
     * Registers a new observer.
     * 
     * @param observer the observer to register
     */
    public void registerObserver(QueueObserver observer);

    /**
     * Removes a observer.
     * 
     * @param observer the observer to remove
     */
    public void removeObserver(QueueObserver observer);

    /**
     * Interface for observer that get notified at specific events.
     */
    public interface QueueObserver {

        /**
         * Called when the observed queue has space available again.
         */
        public void signalNotFull();

        /**
         * Called when the observed queue has new elements available.
         */
        public void signalNotEmpty();

        /**
         * Called when the observed queue has a new element.
         */
        public void signalNewElement();
    }
}
