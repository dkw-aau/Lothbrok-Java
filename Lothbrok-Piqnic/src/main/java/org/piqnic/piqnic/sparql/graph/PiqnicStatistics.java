package org.piqnic.piqnic.sparql.graph;

import org.apache.jena.graph.GraphStatisticsHandler;
import org.apache.jena.graph.Node;

public class PiqnicStatistics implements GraphStatisticsHandler {
    private final PiqnicGraph graph;

    PiqnicStatistics(PiqnicGraph graph) {
        this.graph = graph;
    }

    @Override
    public long getStatistic(Node subject, Node predicate, Node object) {
        return graph.graphBaseSize();
    }
}
