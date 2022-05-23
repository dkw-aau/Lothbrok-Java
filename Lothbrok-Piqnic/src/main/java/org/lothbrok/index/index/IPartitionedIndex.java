package org.lothbrok.index.index;

import org.lothbrok.compatibilitygraph.CompatibilityGraph;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;

import java.util.List;
import java.util.Set;

public interface IPartitionedIndex extends IIndex {
    long estimateCardinality(StarString star);
    long estimateCardinality(StarString star, IGraph fragment);
    //long estimateJoinCardinality(StarString star1, StarString star2);
    //long estimateJoinCardinality(StarString star, List<String> vars, long boundCount);
    //long estimateJoinCardinality(StarString star, List<String> vars, long boundCount, IGraph fragment);
    long estimateJoinCardinality(StarString star, IGraph fragment, IQueryStrategy left);
    CompatibilityGraph getCompatibilityGraph(List<StarString> query);
    IQueryStrategy getQueryStrategy(List<StarString> bgp, CompatibilityGraph graph);
    Set<IGraph> getRelevantFragments(List<StarString> stars);
}
