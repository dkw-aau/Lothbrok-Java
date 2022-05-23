package org.piqnic.index.graph.impl;

import org.piqnic.index.graph.GraphBase;
import org.piqnic.index.util.Triple;
import com.google.gson.Gson;
import org.piqnic.piqnic.node.AbstractNode;
import org.piqnic.piqnic.node.INeighborNode;
import org.piqnic.piqnic.node.impl.NeighborNode;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class Graph extends GraphBase {
    public Graph(String baseUri, String id) {
        super(baseUri, id);
    }
    private final Set<INeighborNode> nodes = new HashSet<>();

    @Override
    public boolean identify(Triple triplePattern) {
        return triplePattern.getPredicate().equals("ANY")
                || triplePattern.getPredicate().startsWith("?")
                || triplePattern.getPredicate().equals(getBaseUri());
    }

    @Override
    public INeighborNode getOneNode() {
        if(nodes.contains(AbstractNode.getState().asNeighborNode())) return AbstractNode.getState().asNeighborNode();
        for(INeighborNode node : nodes) return node;
        return null;
    }

    @Override
    public void addNode(INeighborNode node) {
        this.nodes.add(node);
    }

    @Override
    public void addNodes(Collection<INeighborNode> nodes) {
        this.nodes.addAll(nodes);
    }

    @Override
    public int hashCode() {
        return getBaseUri().hashCode() + getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() != getClass()) return false;
        Graph other = (Graph) obj;
        return getBaseUri().equals(other.getBaseUri()) && getId().equals(other.getId());
    }

    @Override
    public String toString() {
        return toJSONString();
    }

    private String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
