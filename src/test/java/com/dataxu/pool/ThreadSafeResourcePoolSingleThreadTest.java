package com.dataxu.pool;

import org.junit.jupiter.api.*;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class ThreadSafeResourcePoolSingleThreadTest {

    private static int POOL_SIZE = 10;

    @Test
    public void testThePoolCanBeOpenAndClosed() throws GenericResourcePoolException {
        ThreadSafeResourcePool pool = new ThreadSafeResourcePool(POOL_SIZE);
        assertFalse(pool.isOpen());

        pool.open();
        assertTrue(pool.isOpen());

        pool.close();
        assertFalse(pool.isOpen());
    }

    @Test
    public void testThePoolCantBeClosedBeforeOpen() {
        ThreadSafeResourcePool pool = new ThreadSafeResourcePool(POOL_SIZE);
        assertThrows(GenericResourcePoolException.class, pool::close);
    }

    @Test
    public void testThePoolCantBeOpenTwice() {
        ThreadSafeResourcePool pool = new ThreadSafeResourcePool(POOL_SIZE);
        assertThrows(GenericResourcePoolException.class, () -> {
            pool.open();
            pool.open();
        });
    }

    @Test
    public void testThePoolCantBeClosedTwice() {
        ThreadSafeResourcePool pool = new ThreadSafeResourcePool(POOL_SIZE);
        assertThrows(GenericResourcePoolException.class, () -> {
            pool.open();
            pool.close();
            pool.close();
        });
    }

    @Test
    public void testAnObjectCanBeAddedRemoveIntoThePool() throws GenericResourcePoolException {
        ThreadSafeResourcePool<Resource> pool = new ThreadSafeResourcePool<>(POOL_SIZE);
        pool.open();
        Resource resource =  new Resource();
        assertTrue(pool.add(resource));
        assertTrue(pool.remove(resource));
        pool.close();
    }

    @Test
    public void testAnObjectCantBeAddedRemoveFromThePoolTwice() throws GenericResourcePoolException {
        ThreadSafeResourcePool<Resource> pool = new ThreadSafeResourcePool<>(POOL_SIZE);
        pool.open();
        Resource resource =  new Resource();
        assertFalse(pool.remove(resource));
        assertTrue(pool.add(resource));
        assertFalse(pool.add(resource));
        pool.close();
    }

    @Test
    public void testAnObjectCantBeAcquired() throws GenericResourcePoolException {
        ThreadSafeResourcePool<Resource> pool = new ThreadSafeResourcePool<>(POOL_SIZE);
        pool.open();
        Resource resource =  new Resource();
        assertTrue(pool.add(resource));

        Resource acquired = pool.acquire();
        assertNotNull(acquired);

        pool.release(resource);
        pool.close();
    }

    @Test
    public void testThePoolCloseWaitForResourceToBeReleased() throws Exception {
        ThreadSafeResourcePool<Resource> pool = new ThreadSafeResourcePool<>(POOL_SIZE);
        pool.open();
        Resource resource = new Resource();
        try {
            pool.add(resource);
            Thread t1 = new Thread(() -> {
                try {
                    long before = System.currentTimeMillis();
                    pool.close();
                    long after = System.currentTimeMillis();
                    assertTrue((after - before) >= 3000L);
                } catch (Exception e) {
                    fail("An exception occur.");
                }
            });

            Resource acquired = pool.acquire();
            t1.start();
            TimeUnit.MILLISECONDS.sleep(3000);
            pool.release(acquired);

        } catch (Exception e) {
            fail("An exception occur.");
        }
    }

    @Test
    public void testThePoolCloseNowClosesImmediately() throws Exception {
        ThreadSafeResourcePool<Resource> pool = new ThreadSafeResourcePool<>(POOL_SIZE);
        pool.open();
        Resource resource = new Resource();
        try {
            pool.add(resource);
            Thread t1 = new Thread(() -> {
                try {
                    long before = System.currentTimeMillis();
                    pool.closeNow();
                    long after = System.currentTimeMillis();
                    assertTrue((after - before) < 3000L);
                } catch (Exception e) {
                    fail("An exception occur.");
                }
            });

            Resource acquired = pool.acquire();
            t1.start();
            TimeUnit.MILLISECONDS.sleep(3000);
            pool.release(acquired);

        } catch (Exception e) {
            fail("An exception occur.");
        }
    }
}
