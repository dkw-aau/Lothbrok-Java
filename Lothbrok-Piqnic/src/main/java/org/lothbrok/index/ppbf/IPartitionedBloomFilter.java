package org.lothbrok.index.ppbf;

import org.lothbrok.stars.StarString;
import org.lothbrok.utils.Triple;

import java.util.Collection;
import java.util.List;

public interface IPartitionedBloomFilter<T> extends IBloomFilter<T> {
    List<IBloomFilter<T>> getPartitions(StarString star, String var);
    IBloomFilter<T> getPartition(Triple triple, String var);
    IBloomFilter<T> getPartition(StarString star, String var);
    IBloomFilter<T> getSubjectPartition();
    IBloomFilter<T> getPredicatePartition(String predicate);

    boolean mightContainConstants(StarString star);
    boolean mightContainSubject(T element);
    boolean mightContainPredicate(T element);
    boolean mightContainObject(T element, String predicate);
    boolean hasPredicates(Collection<String> predicates);
    void putSubject(T element);
    void putPredicate(T element);
    void putObject(T element, String predicate);
    void deleteFiles();

    void writePredFile();
    boolean hasEmptyPartition();
    void resetPartitionSizes();
    long getTotalCardinality();
}
