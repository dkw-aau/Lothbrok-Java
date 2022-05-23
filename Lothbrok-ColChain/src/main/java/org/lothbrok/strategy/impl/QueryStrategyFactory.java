package org.lothbrok.strategy.impl;

import org.colchain.colchain.community.CommunityMember;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;

import java.util.List;

public class QueryStrategyFactory {
    public static IQueryStrategy buildJoinStrategy(IQueryStrategy sub1, IQueryStrategy sub2) {
        return new JoinQueryStrategy(sub1, sub2);
    }

    public static IQueryStrategy buildJoinStrategy(IQueryStrategy sub1, IQueryStrategy sub2, CommunityMember node) {
        return new JoinQueryStrategy(sub1, sub2, node);
    }

    public static IQueryStrategy buildSingleStrategy(StarString star, IGraph fragment) {
        return new SingleQueryStrategy(star, fragment);
    }

    public static IQueryStrategy buildUnionStrategy(List<IQueryStrategy> strategies) {
        return new UnionQueryStrategy(strategies);
    }

    public static IQueryStrategy buildEmptyStrategy() {
        return new EmptyQueryStrategy();
    }
}
