package org.lothbrok.sparql.graph;

import org.apache.jena.graph.GraphStatisticsHandler;
import org.apache.jena.graph.Node;

public class LothbrokStatistics implements GraphStatisticsHandler {
    private final LothbrokGraph graph;

    LothbrokStatistics(LothbrokGraph graph) {
        this.graph = graph;
    }

    @Override
    public long getStatistic(Node subject, Node predicate, Node object) {
        return graph.graphBaseSize();
    }
}
