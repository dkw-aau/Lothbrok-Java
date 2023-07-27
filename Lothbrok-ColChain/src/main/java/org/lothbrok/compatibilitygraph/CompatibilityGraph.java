package org.lothbrok.compatibilitygraph;

import org.lothbrok.index.graph.IGraph;
import org.lothbrok.stars.StarString;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class CompatibilityGraph {
    private final Set<IGraph> fragments;
    private final Set<CompatibleGraphs> edges;

    public CompatibilityGraph(Set<IGraph> fragments, Set<CompatibleGraphs> edges) {
        this.fragments = fragments;
        this.edges = edges;
    }

    public CompatibilityGraph() {
        this.fragments = new HashSet<>();
        this.edges = new HashSet<>();
    }

    public void addFragment(IGraph fragment) {
        this.fragments.add(fragment);
    }

    public void addEdge(CompatibleGraphs edge) {
        this.edges.add(edge);
    }

    public void addFragments(IGraph fragment1, IGraph fragment2) {
        this.fragments.add(fragment1);
        this.fragments.add(fragment2);
        this.edges.add(new CompatibleGraphs(fragment1, fragment2));
    }

    public void addGraph(CompatibilityGraph other) {
        fragments.addAll(other.fragments);
        edges.addAll(other.edges);
    }

    public boolean isEmpty() {
        return fragments.isEmpty() && edges.isEmpty();
    }

    public boolean hasFragment(StarString star) {
        for(IGraph fragment : fragments) {
            if(fragment.identify(star)) return true;
        }
        return false;
    }

    public Set<IGraph> getFragments() {
        return fragments;
    }

    public boolean hasEdge(IGraph f1, IGraph f2) {
        CompatibleGraphs c = new CompatibleGraphs(f1, f2);
        if(this.edges.contains(c)) return true;
        c = new CompatibleGraphs(f2, f1);
        return this.edges.contains(c);
    }

    public boolean hasEdges(Set<IGraph> f1s, Set<IGraph> f2s) {
        for(IGraph f1 : f1s) {
            for(IGraph f2 : f2s) {
                CompatibleGraphs c = new CompatibleGraphs(f1, f2);
                if(this.edges.contains(c)) return true;
                c = new CompatibleGraphs(f2, f1);
                if(this.edges.contains(c)) return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompatibilityGraph that = (CompatibilityGraph) o;
        return Objects.equals(fragments, that.fragments) && Objects.equals(edges, that.edges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fragments, edges);
    }

    @Override
    public String toString() {
        return "CompatibilityGraph{" +
                "fragments=" + fragments +
                ", edges=" + edges +
                '}';
    }
}
