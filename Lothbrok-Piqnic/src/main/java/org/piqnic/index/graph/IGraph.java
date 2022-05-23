package org.piqnic.index.graph;

import org.piqnic.index.util.Triple;
import org.piqnic.piqnic.node.INeighborNode;

import java.util.Collection;

public interface IGraph {
    boolean identify(Triple triplePattern);
    String getId();
    String getBaseUri();
    INeighborNode getOneNode();
    void addNode(INeighborNode node);
    void addNodes(Collection<INeighborNode> nodes);
}
