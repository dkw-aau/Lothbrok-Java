package org.piqnic.piqnic.node.impl;

import org.piqnic.piqnic.node.INeighborNode;
import org.piqnic.piqnic.node.INode;
import org.lothbrok.index.index.IIndex;

public class NodeFactory {
    public static INode create(String id, String datastore, String address, IIndex index) {
        return new NodeImpl(id, datastore, address, index);
    }

    public static INode create() {
        return new NodeImpl();
    }

    public static INeighborNode createNeighborNode(String address, String id) {
        return new NeighborNode(address, id);
    }
}
