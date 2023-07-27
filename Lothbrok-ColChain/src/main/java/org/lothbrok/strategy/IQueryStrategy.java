package org.lothbrok.strategy;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.colchain.colchain.community.CommunityMember;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.index.IPartitionedIndex;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.stars.StarString;

import java.util.List;
import java.util.Set;

public interface IQueryStrategy {
    ExtendedIterator<Pair<StarString, Binding>> visit(LothbrokGraph graph);
    long estimateCardinality(IPartitionedIndex index);
    List<String> getVars();
    QueryStrategyBase.Type getType();
    boolean isEmpty();
    IGraph getTopFragment();
    List<StarString> getBGP();
    boolean hasNextBinding(LothbrokGraph graph);
    Binding moveToNextBinding(LothbrokGraph graph);
    boolean hasNextBinding(LothbrokGraph graph, LothbrokBindings bindings);
    Binding moveToNextBinding(LothbrokGraph graph, LothbrokBindings bindings);
    Set<IGraph> getFragments();
    Set<IGraph> getTopFragments();
    Set<CommunityMember> getInvolvedNodes();
    StarString getTopStar();
    Set<StarString> getJoiningStars(StarString star);
    Set<IGraph> getJoiningFragments(StarString star);
    long transferCost(CommunityMember node);
    int numBoundVars();
}
