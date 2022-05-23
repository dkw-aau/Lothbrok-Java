package org.lothbrok.sparql.iter;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.colchain.colchain.sparql.ColchainBindings;
import org.colchain.colchain.sparql.ColchainJenaConstants;
import org.colchain.colchain.sparql.exceptions.QueryInterruptedException;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.LothbrokJenaConstants;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.strategy.impl.JoinQueryStrategy;

import java.util.NoSuchElementException;

public class LocalLothbrokJoinIterator extends NiceIterator<Pair<StarString, Binding>> {
    private final ExtendedIterator<Pair<StarString, Binding>> iterator;
    private final IQueryStrategy right;
    private final LothbrokGraph graph;
    private ExtendedIterator<Pair<StarString, Binding>> currStage = null;

    public LocalLothbrokJoinIterator(JoinQueryStrategy strategy, LothbrokBindings bindings, LothbrokGraph graph) {
        this.iterator = graph.graphBaseFind(strategy.getLeft(), bindings);
        this.right = strategy.getRight();
        this.graph = graph;
    }

    private ExtendedIterator<Pair<StarString, Binding>> makeNextStage() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if (this.iterator == null) {
            return null;
        } else {
            LothbrokBindings bindings = new LothbrokBindings();
            for(int i = 0; i < LothbrokJenaConstants.BIND_NUM; i++) {
                if(!this.iterator.hasNext()) break;
                Binding b = this.iterator.next().cdr();
                bindings.add(b);
            }

            if(bindings.size() == 0) {
                this.iterator.close();
                return null;
            }

            return graph.graphBaseFind(right, bindings);
        }
    }

    @Override
    public boolean hasNext() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        while(true) {
            if (this.currStage == null) this.currStage = this.makeNextStage();
            if (this.currStage == null) return false;
            if (this.currStage.hasNext()) return true;

            this.currStage.close();
            this.currStage = null;
        }
    }

    @Override
    public Pair<StarString, Binding> next() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if (!this.hasNext())
            throw new NoSuchElementException(Lib.className(this) + ".next()/finished");
        return this.currStage.next();
    }
}
