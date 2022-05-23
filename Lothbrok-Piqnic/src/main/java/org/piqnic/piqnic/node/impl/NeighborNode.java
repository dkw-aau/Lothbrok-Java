package org.piqnic.piqnic.node.impl;

import org.piqnic.piqnic.node.INeighborNode;

import java.util.Objects;

public class NeighborNode implements INeighborNode {
    private final String address;
    private final String id;

    protected NeighborNode(String address, String id) {
        this.address = address;
        this.id = id;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NeighborNode that = (NeighborNode) o;
        return Objects.equals(address, that.address) && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, id);
    }
}
