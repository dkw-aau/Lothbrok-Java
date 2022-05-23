package org.lothbrok.index.graph;

import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.stars.StarString;
import org.lothbrok.utils.Triple;
import org.piqnic.piqnic.node.INeighborNode;

import java.util.Collection;
import java.util.Set;

public interface IGraph {
    boolean identify(Triple triplePattern);
    boolean identify(StarString starPattern);
    String getId();
    String getBaseUri();
    ICharacteristicSet getCharacteristicSet();
    void addNode(INeighborNode node);
    void addNodes(Collection<INeighborNode> nodes);
    INeighborNode getOneNode();
    Set<INeighborNode> getNeighbors();
}
