package org.piqnic.index.index;

import org.piqnic.index.graph.IGraph;
import org.piqnic.index.ppbf.IBloomFilter;
import org.piqnic.index.util.Triple;

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
