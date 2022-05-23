package org.lothbrok.index.ppbf;

public interface IBloomFilter<T> {
    long elementCount();

    boolean mightContain(T element);

    void put(T element);

    IBloomFilter<T> intersect(IBloomFilter<T> other, String filename);

    IBloomFilter<T> copy();

    void clear();

    boolean isEmpty();

    void deleteFile();

    String getFileName();

    long estimatedCardinality();

    void setNumInsertedElements(long count);
}
