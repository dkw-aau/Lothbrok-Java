package org.lothbrok.sparql.solver;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCheck;
import org.apache.jena.sparql.engine.iterator.QueryIteratorTiming;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.util.Context;

public class LothbrokEngine extends QueryEngineMain {
    protected Query lothbrokQuery;
    protected DatasetGraph lothbrokDataset;
    protected Binding lothbrokBinding;
    protected Context lothbrokContext;

    public LothbrokEngine(Query query, DatasetGraph dataset, Binding input, Context context) {
        super(query, dataset, input, context);
        this.lothbrokQuery = query;
        this.lothbrokDataset = dataset;
        this.lothbrokBinding = input;
        this.lothbrokContext = context;
    }

    public LothbrokEngine(Op op, DatasetGraph dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    // ---- Registration of the factory for this query engine class.

    // Query engine factory.
    // Call PiqnicExtEngine.register() to add to the global query engine registry.

    static QueryEngineFactory factory = new LothbrokEngineFactory();

    static public QueryEngineFactory getFactory() {
        return factory;
    }

    static public void register() {
        QueryEngineRegistry.addFactory(factory);
    }

    static public void unregister() {
        QueryEngineRegistry.removeFactory(factory);
    }

    public QueryIterator eval(Op op, DatasetGraph dsg, Binding input, Context context) {
        ExecutionContext execCxt = new ExecutionContext(context, dsg.getDefaultGraph(), dsg, QC.getFactory(context));
        QueryIterator qIter1 = input.isEmpty() ? QueryIterRoot.create(execCxt) : QueryIterRoot.create(input, execCxt);
        QueryIterator qIter = QC.execute(op, qIter1, execCxt);
        qIter = QueryIteratorCheck.check(qIter, execCxt);

        return qIter;
    }

    static class LothbrokEngineFactory implements QueryEngineFactory {

        @Override
        public boolean accept(Query query, DatasetGraph dataset, Context context) {
            return true;
        }

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding initial, Context context) {
            LothbrokEngine engine = new LothbrokEngine(query, dataset, initial, context);
            return engine.getPlan();
        }

        @Override
        public boolean accept(Op op, DatasetGraph dataset, Context context) {
            // Refuse to accept algebra expressions directly.
            return false;
        }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding inputBinding, Context context) {
            // Should not be called because accept/Op is false
            throw new ARQInternalErrorException("LothbrokQueryEngine: factory called directly with an algebra expression");
        }
    }
}
