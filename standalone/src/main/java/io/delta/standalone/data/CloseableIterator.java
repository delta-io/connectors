package io.delta.standalone.data;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An {@link Iterator} that also need to implement the {@link Closeable} interface. The caller
 * should call {@link #close()} method to free all resources properly after using the iterator.
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable { }
