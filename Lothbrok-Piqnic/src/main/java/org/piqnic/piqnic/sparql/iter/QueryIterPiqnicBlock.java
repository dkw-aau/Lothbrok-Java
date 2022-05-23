package org.piqnic.piqnic.sparql.iter;

import org.piqnic.piqnic.node.AbstractNode;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter1;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.util.FmtUtils;
import org.lothbrok.compatibilitygraph.CompatibilityGraph;
import org.lothbrok.index.index.IPartitionedIndex;
import org.lothbrok.index.index.IndexMapping;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.utils.Tuple;

import java.util.*;

public class QueryIterPiqnicBlock extends QueryIter1 {
    private BasicPattern pattern;
    private QueryIterator output;
    private IndexMapping mapping;

    public static QueryIterator create(QueryIterator input, BasicPattern pattern, ExecutionContext execContext) {
        return new QueryIterPiqnicBlock(input, pattern, execContext);
    }

    private QueryIterPiqnicBlock(QueryIterator input, BasicPattern pattern, ExecutionContext execContext) {
        super(input, execContext);
        this.pattern = pattern;

        /*ReorderTransformation reorder = ReorderLib.fixed();
        if (pattern.size() >= 2 && !input.isJoinIdentity()) {
            QueryIterPeek peek = QueryIterPeek.create(input, execContext);
            input = peek;
            Binding b = peek.peek();
            BasicPattern bgp2 = Substitute.substitute(pattern, b);
            ReorderProc reorderProc = reorder.reorderIndexes(bgp2);
            pattern = reorderProc.reorder(pattern);
        }*/
        QueryIterator chain = input;

        List<org.lothbrok.utils.Triple> triples = new ArrayList<>();
        Map<String, StarString> stars = new HashMap<>();

        for (Triple triple : pattern) {
            triples.add(new org.lothbrok.utils.Triple(triple.getSubject().toString(), triple.getPredicate().toString(), triple.getObject().toString()));
            if(stars.containsKey(triple.getSubject().toString())) {
                stars.get(triple.getSubject().toString()).addTriple(new Tuple<>(triple.getPredicate().toString(), triple.getObject().toString()));
            } else {
                StarString star = new StarString(triple.getSubject().toString());
                star.addTriple(new Tuple<>(triple.getPredicate().toString(), triple.getObject().toString()));
                stars.put(star.getSubject().toString(), star);
            }
        }
        //mapping = AbstractNode.getState().getIndex().getMapping(triples);
        long start = System.currentTimeMillis();
        CompatibilityGraph graph = ((IPartitionedIndex) AbstractNode.getState().getIndex()).getCompatibilityGraph(new ArrayList<>(stars.values()));
        System.out.println(graph.toString());
        System.out.println("Compatibility graph in " + (System.currentTimeMillis() - start) + " ms.");

        IQueryStrategy strategy = ((IPartitionedIndex) AbstractNode.getState().getIndex()).getQueryStrategy(new ArrayList<>(stars.values()), graph);
        System.out.println(strategy.toString());
        System.out.println("Strategy in " + (System.currentTimeMillis() - start) + " ms.");

        for (Triple triple : pattern) {
            chain = new QueryIterPiqnic(chain, triple, execContext,
                    graph.getFragments());
        }


        this.output = chain;
    }

    protected boolean hasNextBinding() {
        return this.output.hasNext();
    }

    protected Binding moveToNextBinding() {
        return this.output.nextBinding();
    }

    protected void closeSubIterator() {
        if (this.output != null) {
            this.output.close();
        }

        this.output = null;
    }

    protected void requestSubCancel() {
        if (this.output != null) {
            this.output.cancel();
        }

    }

    protected void details(IndentedWriter out, SerializationContext sCxt) {
        out.print(Lib.className(this));
        out.println();
        out.incIndent();
        FmtUtils.formatPattern(out, this.pattern, sCxt);
        out.decIndent();
    }
}
