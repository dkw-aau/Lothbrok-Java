package org.lothbrok.strategy.impl;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.index.IPartitionedIndex;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.strategy.QueryStrategyBase;
import org.piqnic.piqnic.node.AbstractNode;
import org.piqnic.piqnic.node.INeighborNode;

import java.util.*;

public class JoinQueryStrategy  extends QueryStrategyBase {
    private final IQueryStrategy left;
    private final IQueryStrategy right;
    private final INeighborNode node;

    JoinQueryStrategy(IQueryStrategy left, IQueryStrategy right) {
        super(Type.JOIN);
        this.left = left;
        this.right = right;
        //this.node = node;

        Set<INeighborNode> nodes = getNodesByFragments(this.getTopFragments());
        long lowest = Long.MAX_VALUE;
        INeighborNode bestNode = null;
        for(INeighborNode node : nodes) {
            IQueryStrategy strat = QueryStrategyFactory.buildJoinStrategy(left, right, node);
            long cost = strat.transferCost(node);
            if(cost < lowest) {
                bestNode = node;
                lowest = cost;
            }
        }

        this.node = bestNode;
    }

    JoinQueryStrategy(IQueryStrategy left, IQueryStrategy right, INeighborNode node) {
        super(Type.JOIN);
        this.left = left;
        this.right = right;
        this.node = node;
    }

    private Set<INeighborNode> getNodesByFragments(Set<IGraph> fragments) {
        Set<INeighborNode> nodes = new HashSet<>();

        for(IGraph fragment : fragments) {
            nodes.addAll(fragment.getNeighbors());
        }

        return nodes;
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

    @Override
    public List<String> getVars() {
        List<String> vars = new ArrayList<>(left.getVars());
        vars.addAll(right.getVars());
        return vars;
    }

    @Override
    public StarString getTopStar() {
        return right.getTopStar();
    }

    @Override
    public Set<IGraph> getTopFragments() {
        Set<IGraph> fs = new HashSet<>();
        fs.addAll(right.getFragments());
        return fs;
    }

    @Override
    public long estimateCardinality(IPartitionedIndex index) {
        long card = 0;
        if (this.left.getType() == Type.UNION) {
            for (IQueryStrategy strategy : ((UnionQueryStrategy) left).getStrategies()) {
                IQueryStrategy strat = QueryStrategyFactory.buildJoinStrategy(strategy, right);
                card += strat.estimateCardinality(index);
            }
        } else if (this.right.getType() == Type.UNION) {
            for (IQueryStrategy strategy : ((UnionQueryStrategy) right).getStrategies()) {
                IQueryStrategy strat = QueryStrategyFactory.buildJoinStrategy(left, strategy);
                card += strat.estimateCardinality(index);
            }
        } else {
            card = index.estimateJoinCardinality(((SingleQueryStrategy) right).getStar(), ((SingleQueryStrategy) right).getFragment(), left);
        }

        return card;
    }

    private boolean joins(List<String> v1, List<String> v2) {
        for(String s1 : v1) {
            for(String s2 : v2) {
                if(s1.equals(s2)) return true;
            }
        }
        return false;
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
    public String toString() {
        return "(" + left + ") Join (" + right + ")";
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
    public Set<INeighborNode> getInvolvedNodes() {
        Set<INeighborNode> nodes = left.getInvolvedNodes();
        nodes.addAll(right.getInvolvedNodes());
        nodes.add(getTopFragment().getOneNode());

        return nodes;
    }

    @Override
    public long transferCost(INeighborNode node) {
        long cost = 0;

        if (this.right.getType() == Type.UNION) {
            for (IQueryStrategy strategy : ((UnionQueryStrategy) right).getStrategies()) {
                IQueryStrategy strat = QueryStrategyFactory.buildJoinStrategy(left, strategy);
                cost += strat.transferCost(node);
            }
        } else {
            cost = left.transferCost(this.node);
            if (!((SingleQueryStrategy) right).getInvolvedNodes().contains(node))
                cost += estimateCardinality((IPartitionedIndex) AbstractNode.getState().getIndex());
        }

        if (!node.equals(this.node))
            cost += estimateCardinality((IPartitionedIndex) AbstractNode.getState().getIndex());

        return cost;
    }
}
