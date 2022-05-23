package org.lothbrok.compatibilitygraph;

import org.lothbrok.index.graph.IGraph;
import org.lothbrok.utils.Tuple;

import java.util.Objects;

public class CompatibleGraphs {
    private final IGraph first;
    private final IGraph second;

    public CompatibleGraphs(IGraph first, IGraph second) {
        this.first = first;
        this.second = second;
    }

    public Tuple<IGraph,IGraph> getFragments() {
        return new Tuple<>(first, second);
    }

    public boolean matches(IGraph first, IGraph second) {
        return (this.first.equals(first) && this.second.equals(second))
                || (this.first.equals(second) && this.second.equals(first));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompatibleGraphs that = (CompatibleGraphs) o;
        return Objects.equals(first, that.first) && Objects.equals(second, that.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
}
