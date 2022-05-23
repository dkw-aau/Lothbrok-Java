package org.lothbrok.strategy.impl;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.index.IPartitionedIndex;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.sparql.iter.EmptyIterator;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.QueryStrategyBase;
import org.piqnic.piqnic.node.INeighborNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EmptyQueryStrategy extends QueryStrategyBase {
    EmptyQueryStrategy() {
        super(Type.EMPTY);
    }

    @Override
    public ExtendedIterator<Pair<StarString, Binding>> visit(LothbrokGraph graph) {
        return null;
    }

    @Override
    public long estimateCardinality(IPartitionedIndex index) {
        return 0;
    }

    @Override
    public Set<IGraph> getTopFragments() {
        return null;
    }

    @Override
    public StarString getTopStar() {
        return null;
    }

    @Override
    public Set<StarString> getJoiningStars(StarString star) {
        return null;
    }

    @Override
    public Set<IGraph> getJoiningFragments(StarString star) {
        return null;
    }

    @Override
    public long transferCost(INeighborNode node) {
        return 0;
    }

    @Override
    public List<String> getVars() {
        return new ArrayList<>();
    }

    @Override
    public IGraph getTopFragment() {
        return null;
    }

    @Override
    public boolean hasNextBinding(LothbrokGraph graph) {
        return false;
    }

    @Override
    public Binding moveToNextBinding(LothbrokGraph graph) {
        return null;
    }

    @Override
    public boolean hasNextBinding(LothbrokGraph graph, LothbrokBindings bindings) {
        return false;
    }

    @Override
    public Binding moveToNextBinding(LothbrokGraph graph, LothbrokBindings bindings) {
        return null;
    }

    @Override
    public List<StarString> getBGP() {
        return new ArrayList<>();
    }

    @Override
    public Set<IGraph> getFragments() {
        return new HashSet<>();
    }

    @Override
    public Set<INeighborNode> getInvolvedNodes() {
        return new HashSet<>();
    }
}
