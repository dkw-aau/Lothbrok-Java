package org.lothbrok.index.index;

import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.ppbf.IBloomFilter;
import org.lothbrok.utils.Triple;

import java.util.List;
import java.util.Set;

public interface IIndex {
    boolean isBuilt();
    IndexMapping getMapping(List<Triple> query);
    void addFragment(IGraph graph, IBloomFilter<String> filter);
    Set<IGraph> getGraphs();
    IGraph getGraph(String id);
    boolean hasFragment(String fid);
    String getPredicate(String id);
    List<String> getByPredicate(String predicate);
}
