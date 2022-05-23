package org.lothbrok.strategy.dp;

import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;

import java.util.List;

public class DpTableEntry {
    private final List<StarString> subquery;
    private final IQueryStrategy strategy;
    private final long cost;

    public DpTableEntry(List<StarString> subquery, IQueryStrategy strategy, long cost) {
        this.subquery = subquery;
        this.strategy = strategy;
        this.cost = cost;
    }

    public List<StarString> getSubquery() {
        return subquery;
    }

    public IQueryStrategy getStrategy() {
        return strategy;
    }

    public long getCost() {
        return cost;
    }

    public boolean isSubquery(List<StarString> subquery) {
        return this.subquery.containsAll(subquery) && subquery.containsAll(this.subquery);
    }

    public boolean containsStar(StarString star) {
        return subquery.contains(star);
    }

    @Override
    public String toString() {
        return "DpTableEntry{" +
                "subquery=" + subquery +
                ", strategy=" + strategy +
                ", cost=" + cost +
                '}';
    }
}
