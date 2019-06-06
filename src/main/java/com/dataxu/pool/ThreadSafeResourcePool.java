package com.dataxu.pool;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.Objects.requireNonNull;

/**
 * Thread-safe resources pool. Acquire and release methods are not synchronized which should produce a good performance.
 */
public class ThreadSafeResourcePool<T> implements IGenericResourcePool<T> {

    private BlockingQueue<Wrapper<T>> pool;

    private Map<T, Lock> locks;

    private volatile AtomicBoolean open = new AtomicBoolean(false);

    private volatile boolean closeCalled;

    /**
     * Constructor.
     *
     * @param size size of the pull.
     */
    public ThreadSafeResourcePool(int size) {
        this.pool = new ArrayBlockingQueue<>(size);
        this.locks = new ConcurrentHashMap<>(size);
    }

    @Override
    public synchronized void open() throws GenericResourcePoolException {
        boolean wasOpen = this.open.getAndSet(true);
        if (wasOpen) {
            throw new GenericResourcePoolException("The locks is already open");
        }
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public synchronized void close() throws GenericResourcePoolException {
        close(false);
    }

    @Override
    public void closeNow() throws GenericResourcePoolException {
        close(true);
    }

    private void close(boolean force) throws GenericResourcePoolException {
        boolean wasOpen = this.open.getAndSet(false);
        if (!wasOpen) {
            throw new GenericResourcePoolException(poolStateError());
        } else {
            closeCalled = true;
            releaseLocks(force);
        }
    }

    @Override
    public T acquire() throws GenericResourcePoolException {
        if (isOpen()) {
            try {
                Wrapper<T> wrapper;
                do {
                    wrapper = pool.take();
                } while (wrapper.getLock() != null && !closeCalled && !wrapper.getLock().tryLock());

                return wrapper.getResource();

            } catch (InterruptedException e) {
                return null;
            }
        }
        throw new GenericResourcePoolException(poolStateError());
    }

    @Override
    public T acquire(long timeout, TimeUnit unit) throws GenericResourcePoolException {
        // Save current time, we will use it later to reduce blocking time
        long beforeMilliseconds = System.currentTimeMillis();

        if (isOpen()) {

            // Convert all units into milliseconds
            long milliseconds = unit.convert(timeout, TimeUnit.MILLISECONDS);

            try {
                Wrapper<T> wrapper;
                do {
                    // Pool method maybe called multiple times if multiple thread simultaneously were getting the same object
                    // We need to reflect that by reducing the total wait time.
                    long currentMilliseconds = System.currentTimeMillis();
                    long spendMilliseconds = currentMilliseconds - beforeMilliseconds;

                    wrapper = pool.poll(milliseconds - spendMilliseconds, TimeUnit.MILLISECONDS);

                    // Wrapper object will be null after timeout, we are throwing an exception here
                    if (wrapper == null) {
                        throw new GenericResourcePoolException("Timeout exception");
                    }

                } while (wrapper.getLock() != null && !requireNonNull(wrapper).getLock().tryLock());

                if (wrapper.getResource() == null) {
                    pool.offer(new Wrapper<>());
                }
                return wrapper.getResource();

            } catch (InterruptedException e) {
                throw new GenericResourcePoolException(e);
            }
        }

        throw new GenericResourcePoolException(poolStateError());
    }

    @Override
    public void release(T resource) {
        if (isOpen()) {
            // Checks if a resource was added into a pool
            Lock lock = locks.get(resource);
            if (lock != null) {
                lock.unlock();
                pool.offer(new Wrapper<>(resource, lock));
            }
        }
    }

    @Override
    public synchronized boolean add(T resource) throws GenericResourcePoolException {
        Wrapper<T> wrapper = new Wrapper<>(resource);
        // Since that add is not called very frequently it is OK to check contains method
        if (pool.contains(wrapper)) {
            return false;
        }
        try {
            pool.put(wrapper);
            locks.put(wrapper.getResource(), wrapper.getLock());
        } catch (InterruptedException e) {
            throw new GenericResourcePoolException("Timeout.");
        }
        return true;
    }

    @Override
    public synchronized boolean remove(T resource) throws GenericResourcePoolException {
        if (isOpen()) {
            Lock lock = this.locks.get(resource);
            if (lock != null) {
                lock.lock();
                return removeNow(resource);
            }
            return false;
        }
        throw new GenericResourcePoolException(poolStateError());
    }

    @Override
    public boolean removeNow(T resource) throws GenericResourcePoolException {
        if (isOpen()) {
            boolean removed = pool.remove(new Wrapper<>(resource));
            if (removed) {
                this.locks.remove(resource);
                return true;
            }
            return false;
        }
        throw new GenericResourcePoolException(poolStateError());
    }

    private String poolStateError() {
        return this.closeCalled ? "Object pool is closed" : "Object pool is not open";
    }

    private void releaseLocks(boolean force) {
        locks.forEach((key, lock) -> {
            if (force) {
                // A BlockingQueue does not intrinsically support any kind of "close" or "shutdown" operation
                // to indicate that no more items will be added.
                pool.offer(new Wrapper<>());
            } else {
                // Wait when lock will be released.
                lock.lock();
                lock.unlock();
            }
        });
        locks.clear();
        pool.clear();
    }

    class Wrapper<R> {

        private R resource;
        private Lock lock;

        /**
         * This constructor is a "End of Blocking Queue marker". If acquire() return a wrapper with empty lock, it means that Pool was closed
         * and we need to unblock all waiting threads.
         */
        public Wrapper() {
        }

        public Wrapper(R resource) {
            this.resource = resource;
            this.lock = new ReentrantLock();
        }

        public Wrapper(R resource, Lock lock) {
            this.resource = resource;
            this.lock = lock;
        }

        public R getResource() {
            return resource;
        }

        public Lock getLock() {
            return lock;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Wrapper<?> wrapper = (Wrapper<?>) o;
            return resource.equals(wrapper.resource);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resource);
        }
    }
}
