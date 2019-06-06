package com.dataxu.pool;

import java.util.concurrent.*;

public interface IGenericResourcePool<T> {

    void open() throws GenericResourcePoolException;

    void close() throws GenericResourcePoolException;

    void closeNow() throws GenericResourcePoolException;

    boolean isOpen();

    T acquire() throws GenericResourcePoolException;

    T acquire(long timeout, TimeUnit unit) throws GenericResourcePoolException;

    void release(T resource) throws GenericResourcePoolException;

    boolean add(T resource) throws GenericResourcePoolException;

    boolean remove(T resource) throws GenericResourcePoolException;

    boolean removeNow(T resource) throws GenericResourcePoolException;
}
