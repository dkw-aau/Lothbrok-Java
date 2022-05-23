package org.lothbrok.strategy.impl;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.node.AbstractNode;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.index.IPartitionedIndex;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.QueryStrategyBase;

import java.util.*;

public class SingleQueryStrategy extends QueryStrategyBase {
    private final StarString star;
    private final IGraph fragment;

    SingleQueryStrategy(StarString star, IGraph fragment) {
        super(Type.SINGLE);
        this.star = star;
        this.fragment = fragment;
    }

    public StarString getStar() {
        return star;
    }

    public IGraph getFragment() {
        return fragment;
    }

    @Override
    public ExtendedIterator<Pair<StarString, Binding>> visit(LothbrokGraph graph) {
        return null;
    }

    //@Override
    //public long estimateCardinality(IPartitionedIndex index, List<String> vars, long boundCount) {
    //    return index.estimateJoinCardinality(star, vars, boundCount, fragment);
    //}

    @Override
    public List<String> getVars() {
        return star.getVariables();
    }

    @Override
    public StarString getTopStar() {
        return star;
    }

    @Override
    public Set<StarString> getJoiningStars(StarString star) {
        Set<StarString> set = new HashSet<>();
        Set<String> vars = new HashSet<>(star.getVariables());
        vars.retainAll(this.star.getVariables());
        if(vars.size() > 0) set.add(this.star);
        return set;
    }

    @Override
    public Set<IGraph> getJoiningFragments(StarString star) {
        Set<IGraph> set = new HashSet<>();
        Set<String> vars = new HashSet<>(star.getVariables());
        vars.retainAll(this.star.getVariables());
        if(vars.size() > 0) set.add(this.fragment);
        return set;
    }

    @Override
    public Set<IGraph> getTopFragments() {
        return getFragments();
    }

    @Override
    public long estimateCardinality(IPartitionedIndex index) {
        return index.estimateCardinality(star, fragment);
    }

    @Override
    public IGraph getTopFragment() {
        return fragment;
    }

    @Override
    public List<StarString> getBGP() {
        List<StarString> bgp = new ArrayList<>();
        bgp.add(star);
        return bgp;
    }

    @Override
    public String toString() {
        return "[[" + star.getSubject().toString() + "]]" + fragment.getId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SingleQueryStrategy that = (SingleQueryStrategy) o;
        return Objects.equals(star, that.star) && Objects.equals(fragment, that.fragment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(star, fragment);
    }

    @Override
    public Set<IGraph> getFragments() {
        Set<IGraph> fs = new HashSet<>();
        fs.add(fragment);
        return fs;
    }

    @Override
    public Set<CommunityMember> getInvolvedNodes() {
        Set<CommunityMember> nodes = new HashSet<>();
        nodes.add(AbstractNode.getState().getCommunity(fragment.getCommunity()).getParticipant());
        return nodes;
    }

    @Override
    public long transferCost(CommunityMember node) {
        if(AbstractNode.getState().getCommunityByFragmentId(fragment.getId()).getParticipants().contains(node))
            return 0;
        return ((IPartitionedIndex)AbstractNode.getState().getIndex()).estimateCardinality(star, fragment);
    }
}
