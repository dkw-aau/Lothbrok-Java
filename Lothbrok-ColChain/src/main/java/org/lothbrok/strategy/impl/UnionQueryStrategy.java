package org.lothbrok.strategy.impl;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.colchain.colchain.community.CommunityMember;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.index.IPartitionedIndex;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.strategy.QueryStrategyBase;

import java.util.*;

public class UnionQueryStrategy extends QueryStrategyBase {
    private final List<IQueryStrategy> strategies;

    UnionQueryStrategy(List<IQueryStrategy> strategies) {
        super(Type.UNION);
        this.strategies = strategies;
    }

    public List<IQueryStrategy> getStrategies() {
        return strategies;
    }

    public void addStrategy(IQueryStrategy strategy) {
        strategies.add(strategy);
    }

    @Override
    public ExtendedIterator<Pair<StarString, Binding>> visit(LothbrokGraph graph) {
        return null;
    }

//    @Override
//    public long estimateCardinality(IPartitionedIndex index, List<String> vars, long boundCount) {
//        long cnt = 0;
//        for(IQueryStrategy strategy : strategies) {
//            cnt += strategy.estimateCardinality(index, vars, boundCount);
//        }
//        return cnt;
//    }

    @Override
    public List<String> getVars() {
        Set<String> vars = new HashSet<>();
        for(IQueryStrategy strategy : strategies) {
            vars.addAll(strategy.getVars());
        }
        return new ArrayList<>(vars);
    }

    @Override
    public long estimateCardinality(IPartitionedIndex index) {
        long cnt = 0;
        for(IQueryStrategy strategy : strategies) {
            cnt += strategy.estimateCardinality(index);
        }
        return cnt;
    }

    @Override
    public StarString getTopStar() {
        return strategies.get(0).getTopStar();
    }

    @Override
    public Set<StarString> getJoiningStars(StarString star) {
        Set<StarString> set = new HashSet<>();
        for(IQueryStrategy strat : strategies) {
            set.addAll(strat.getJoiningStars(star));
        }
        return set;
    }

    @Override
    public Set<IGraph> getJoiningFragments(StarString star) {
        Set<IGraph> set = new HashSet<>();
        for(IQueryStrategy strat : strategies) {
            set.addAll(strat.getJoiningFragments(star));
        }
        return set;
    }

    @Override
    public IGraph getTopFragment() {
        if(this.strategies.isEmpty()) return null;
        return this.strategies.get(0).getTopFragment();
    }

    @Override
    public List<StarString> getBGP() {
        Set<StarString> bgp = new HashSet<>();
        for(IQueryStrategy strategy : strategies) {
            bgp.addAll(strategy.getBGP());
        }
        return new ArrayList<>(bgp);
    }

    @Override
    public String toString() {
        if(strategies.isEmpty()) return "()";
        String str = "(" + strategies.get(0) + ")";
        for(int i = 1; i < strategies.size(); i++) {
            str += " UNION (" + strategies.get(i) + ")";
        }
        return str;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnionQueryStrategy that = (UnionQueryStrategy) o;
        return Objects.equals(strategies, that.strategies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(strategies);
    }

    @Override
    public Set<IGraph> getFragments() {
        Set<IGraph> fs = new HashSet<>();
        for(IQueryStrategy strat : strategies) fs.addAll(strat.getFragments());
        return fs;
    }

    @Override
    public Set<IGraph> getTopFragments() {
        return getFragments();
    }

    @Override
    public Set<CommunityMember> getInvolvedNodes() {
        Set<CommunityMember> nodes = new HashSet<>();
        for(IQueryStrategy strategy : strategies) {
            nodes.addAll(strategy.getInvolvedNodes());
        }
        return nodes;
    }

    @Override
    public long transferCost(CommunityMember node) {
        long cost = 0;
        for(IQueryStrategy strategy : strategies) {
            cost += strategy.transferCost(node);
        }
        return cost;
    }
}
