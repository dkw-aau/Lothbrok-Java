package org.lothbrok.sparql.iter;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.LothbrokJenaConstants;
import org.lothbrok.sparql.exceptions.QueryInterruptedException;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public class QueryIterLothbrok extends QueryIterRepeatApply {
    private final IQueryStrategy strategy;
    private final ExecutionContext cxt;
    int count = 0;

    public QueryIterLothbrok(QueryIterator input, IQueryStrategy strategy, ExecutionContext cxt) {
        super(input, cxt);
        this.strategy = strategy;
        this.cxt = cxt;
    }

    protected QueryIterator nextStage(Binding binding) {
        return null;
    }

    @Override
    protected boolean hasNextBinding() {
        if (Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if (this.isFinished()) {
            return false;
        }
        return this.strategy.hasNextBinding((LothbrokGraph) cxt.getActiveGraph());
    }

    @Override
    protected Binding moveToNextBinding() {
        if (Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if (!this.hasNextBinding()) {
            throw new NoSuchElementException(Lib.className(this) + ".next()/finished");
        } else {
            return this.strategy.moveToNextBinding((LothbrokGraph) cxt.getActiveGraph());
        }
    }

    /*static class BindMapper extends QueryIter {
        private final IQueryStrategy strategy;
        private Binding slot = null;
        private boolean finished = false;
        private volatile boolean cancelled = false;
        private ExtendedIterator<Pair<StarString, Binding>> iter;

        BindMapper(LothbrokBindings bindings, IQueryStrategy strategy, ExecutionContext cxt) {
            super(cxt);

            this.strategy = strategy;
            LothbrokGraph g = (LothbrokGraph) cxt.getActiveGraph();
            iter = g.graphBaseFind(pattern, bindings, fragments);
        }

        private Binding mapper(Triple r, Binding b) {
            BindingMap results = BindingFactory.create(b);
            if (!insert(this.s, r.getSubject(), results)) {
                return null;
            } else if (!insert(this.p, r.getPredicate(), results)) {
                return null;
            } else {
                return !insert(this.o, r.getObject(), results) ? null : results;
            }
        }

        private static boolean insert(Node inputNode, Node outputNode, BindingMap results) {
            if (!Var.isVar(inputNode)) {
                return true;
            } else {
                Var v = Var.alloc(inputNode);
                Node x = results.get(v);
                if (x != null) {
                    return outputNode.equals(x);
                } else {
                    results.add(v, outputNode);
                    return true;
                }
            }
        }

        protected boolean hasNextBinding() {
            if (Thread.interrupted())
                throw new QueryInterruptedException("Interrupted.");
            if (this.finished) {
                return false;
            } else if (this.slot != null) {
                return true;
            } else if (this.cancelled) {
                this.finished = true;
                return false;
            } else {
                while (this.iter.hasNext() && this.slot == null) {
                    Pair<Triple, Binding> pair = this.iter.next();
                    Binding b = this.mapper(pair.car(), pair.cdr());
                    this.slot = b;
                }

                if (this.slot == null) {
                    this.finished = true;
                }

                return this.slot != null;
            }
        }

        protected Binding moveToNextBinding() {
            if (Thread.interrupted())
                throw new QueryInterruptedException("Interrupted.");
            if (!this.hasNextBinding()) {
                throw new ARQInternalErrorException();
            } else {
                Binding r = this.slot;
                this.slot = null;
                return r;
            }
        }

        protected void closeIterator() {
        }

        protected void requestCancel() {
            this.cancelled = true;
        }
    }*/
}
