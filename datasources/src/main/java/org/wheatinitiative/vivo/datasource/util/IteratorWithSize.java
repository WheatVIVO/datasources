package org.wheatinitiative.vivo.datasource.util;

import java.util.Iterator;

public interface IteratorWithSize<T> extends Iterator<T> {

    /**
     * Get the size of the collection over which the iterator is iterating,
     * otherwise null if unknown
     * @return size as Integer; null if size is unknown
     */
    public Integer size();
    
}
