package com.dataxu.pool;

import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import static org.junit.jupiter.api.Assertions.*;

public class ThreadSafeResourcePoolConcurrentTest {

    private static int POOL_SIZE = 10;

    @Test
    public void testThePoolLocksAnObjectUntilItWasReleased() throws Exception {
        runTestInOpenPool(pool -> {
            Resource resource = new Resource();
            try {
                pool.add(resource);
                Thread t1 = new Thread(() -> {
                    try {
                        Resource acquired = pool.acquire();

                        assertNotNull(acquired);
                        assertEquals(resource, acquired);

                        TimeUnit.SECONDS.sleep(3);
                        pool.release(acquired);
                    } catch (Exception e) {
                        fail("An exception occur.");
                    }
                });
                Thread t2 = new Thread(() -> {
                    try {
                        // Wait until first thread acquires lock
                        TimeUnit.SECONDS.sleep(1);
                        long before = System.currentTimeMillis();
                        Resource acquired = pool.acquire();
                        long after = System.currentTimeMillis();

                        assertTrue((after - before) >= 2000L); // Waiting time is more than 2 seconds
                        assertNotNull(acquired);
                        assertEquals(resource, acquired);

                        pool.release(acquired);
                    } catch (Exception e) {
                        fail("An exception occur.");
                    }
                });

                t1.start();
                t2.start();

                t1.join();
                t2.join();

            } catch (Exception e) {
                fail("An exception occur.");
            }
        });
    }

    @Test
    public void testThePoolCanBeAccessedWithLargeNumberOfThreads() throws Exception {
        runTestInOpenPool(pool -> {
            Resource resource = new Resource();
            try {
                int threadsCount = 100;
                pool.add(resource);

                final CountDownLatch latch = new CountDownLatch(threadsCount);
                List<Thread> threads = new ArrayList<>();
                for (int i = 0 ; i < threadsCount ; i++) {
                    threads.add(new Thread(() -> {
                        try {
                            Resource acquired = pool.acquire();
                            TimeUnit.MILLISECONDS.sleep(10);
                            pool.release(acquired);

                            latch.countDown();
                        } catch (Exception e) {
                            fail("An exception occur.");
                        }
                    }));
                }

                for (Thread t : threads) {
                    t.start();
                }

                latch.await();

            } catch (Exception e) {
                fail("An exception occur.");
            }
        });
    }

    @Test
    public void testThePoolCanBeAccessedWithLargeNumberOfThreadsWIthTimeout() throws Exception {
        runTestInOpenPool(pool -> {
            Resource resource = new Resource();
            try {
                int threadsCount = 2;
                pool.add(resource);
                List<Thread> threads = new ArrayList<>();
                for (int i = 0 ; i < threadsCount ; i++) {
                    threads.add(new Thread(() -> {
                        assertThrows(GenericResourcePoolException.class, () -> {
                            pool.acquire(100, TimeUnit.MILLISECONDS);
                        });
                    }));
                }

                pool.acquire();

                for (Thread t : threads) {
                    t.start();
                }

                TimeUnit.MILLISECONDS.sleep(120);

            } catch (Exception e) {
                fail("An exception occur.");
            }
        });
    }

    private void runTestInOpenPool(Consumer<ThreadSafeResourcePool<Resource>> runTestForPool) throws Exception {
        ThreadSafeResourcePool<Resource> pool = new ThreadSafeResourcePool<>(POOL_SIZE);
        pool.open();
        runTestForPool.accept(pool);
        pool.close();
    }
}
