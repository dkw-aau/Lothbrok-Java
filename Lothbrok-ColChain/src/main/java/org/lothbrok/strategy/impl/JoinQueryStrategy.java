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
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.strategy.QueryStrategyBase;

import java.util.*;

public class JoinQueryStrategy extends QueryStrategyBase {
    private final IQueryStrategy left;
    private final IQueryStrategy right;
    private final CommunityMember node;

    JoinQueryStrategy(IQueryStrategy left, IQueryStrategy right) {
        super(Type.JOIN);
        this.left = left;
        this.right = right;
        //this.node = node;

        Set<CommunityMember> nodes = AbstractNode.getState().getNodesByFragments(this.getTopFragments());
        long lowest = Long.MAX_VALUE;
        CommunityMember bestNode = null;
        for(CommunityMember node : nodes) {
            IQueryStrategy strat = QueryStrategyFactory.buildJoinStrategy(left, right, node);
            long cost = strat.transferCost(node);
            if(cost < lowest) {
                bestNode = node;
                lowest = cost;
            }
        }

        this.node = bestNode;
    }

    JoinQueryStrategy(IQueryStrategy left, IQueryStrategy right, CommunityMember node) {
        super(Type.JOIN);
        this.left = left;
        this.right = right;
        this.node = node;
    }

    public IQueryStrategy getLeft() {
        return left;
    }

    public IQueryStrategy getRight() {
        return right;
    }

    //public CommunityMember getNode() {
    //    return node;
    //}

    @Override
    public ExtendedIterator<Pair<StarString, Binding>> visit(LothbrokGraph graph) {
        return null;
    }

//    @Override
//    public long estimateCardinality(IPartitionedIndex index, List<String> vars, long boundCount) {
//        long card = left.estimateCardinality(index, vars, boundCount);
//        List<String> nvars = new ArrayList<>(vars);
//        nvars.addAll(left.getVars());
//        return right.estimateCardinality(index, nvars, card);
//    }

    @Override
    public List<String> getVars() {
        List<String> vars = new ArrayList<>(left.getVars());
        vars.addAll(right.getVars());
        return vars;
    }

    @Override
    public long estimateCardinality(IPartitionedIndex index) {
        long card = 0;
        if (this.right.getType() == Type.UNION) {
            for (IQueryStrategy strategy : ((UnionQueryStrategy) right).getStrategies()) {
                IQueryStrategy strat = QueryStrategyFactory.buildJoinStrategy(left, strategy);
                card += strat.estimateCardinality(index);
            }
        } else {
            card = index.estimateJoinCardinality(((SingleQueryStrategy) right).getStar(), ((SingleQueryStrategy) right).getFragment(), left);
        }

        return card;
    }

    @Override
    public StarString getTopStar() {
        return right.getTopStar();
    }

    @Override
    public IGraph getTopFragment() {
        return right.getTopFragment();
    }

    @Override
    public List<StarString> getBGP() {
        Set<StarString> bgp = new HashSet<>(left.getBGP());
        bgp.addAll(right.getBGP());
        return new ArrayList<>(bgp);
    }

    @Override
    public String toString() {
        return "(" + left + ") Join[" + node.getAddress() + "] (" + right + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinQueryStrategy that = (JoinQueryStrategy) o;
        return Objects.equals(left, that.left) && Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    @Override
    public Set<IGraph> getFragments() {
        Set<IGraph> fs = new HashSet<>();
        fs.addAll(left.getFragments());
        fs.addAll(right.getFragments());
        return fs;
    }

    @Override
    public Set<IGraph> getTopFragments() {
        Set<IGraph> fs = new HashSet<>();
        fs.addAll(right.getFragments());
        return fs;
    }

    public CommunityMember getNode() {
        return node;
    }

    @Override
    public Set<StarString> getJoiningStars(StarString star) {
        Set<StarString> set = new HashSet<>();
        set.addAll(left.getJoiningStars(star));
        set.addAll(right.getJoiningStars(star));
        return set;
    }

    @Override
    public Set<IGraph> getJoiningFragments(StarString star) {
        Set<IGraph> set = new HashSet<>();
        set.addAll(left.getJoiningFragments(star));
        set.addAll(right.getJoiningFragments(star));
        return set;
    }

    @Override
    public Set<CommunityMember> getInvolvedNodes() {
        Set<CommunityMember> nodes = left.getInvolvedNodes();
        nodes.addAll(right.getInvolvedNodes());
        nodes.add(AbstractNode.getState().getCommunity(getTopFragment().getCommunity()).getParticipant());

        return nodes;
    }

    @Override
    public long transferCost(CommunityMember node) {
        long cost = 0;

        if (this.right.getType() == Type.UNION) {
            for (IQueryStrategy strategy : ((UnionQueryStrategy) right).getStrategies()) {
                IQueryStrategy strat = QueryStrategyFactory.buildJoinStrategy(left, strategy);
                cost += strat.transferCost(node);
            }
        } else {
            cost = left.transferCost(this.node);
            if (!AbstractNode.getState().getCommunityByFragmentId(((SingleQueryStrategy) right).getFragment().getId()).getParticipants().contains(node))
                cost += estimateCardinality((IPartitionedIndex) AbstractNode.getState().getIndex());
        }

        if (!node.equals(this.node))
            cost += estimateCardinality((IPartitionedIndex) AbstractNode.getState().getIndex());

        return cost;
    }
}
