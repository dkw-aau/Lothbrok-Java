package org.lothbrok.sparql.iter;

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
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.sparql.LothbrokJenaConstants;
import org.piqnic.piqnic.node.AbstractNode;
import org.lothbrok.compatibilitygraph.CompatibilityGraph;
import org.lothbrok.index.index.IPartitionedIndex;
import org.lothbrok.index.index.IndexMapping;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.utils.Tuple;
import org.piqnic.piqnic.node.INeighborNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class QueryIterLothbrokBlock extends QueryIter1 {
    private BasicPattern pattern;
    private QueryIterator output;

    public static QueryIterator create(QueryIterator input, BasicPattern pattern, ExecutionContext execContext) {
        return new QueryIterLothbrokBlock(input, pattern, execContext);
    }

    private QueryIterLothbrokBlock(QueryIterator input, BasicPattern pattern, ExecutionContext execContext) {
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
        Map<String, StarString> stars = new HashMap<>();

        for (Triple triple : pattern) {
            if (stars.containsKey(triple.getSubject().toString())) {
                stars.get(triple.getSubject().toString()).addTriple(new Tuple<>(triple.getPredicate().toString(), triple.getObject().toString()));
            } else {
                StarString star = new StarString(triple.getSubject().toString());
                star.addTriple(new Tuple<>(triple.getPredicate().toString(), triple.getObject().toString()));
                stars.put(star.getSubject().toString(), star);
            }
        }
        Set<IGraph> relFragments = ((IPartitionedIndex) AbstractNode.getState().getIndex()).getRelevantFragments(new ArrayList<>(stars.values()));
        LothbrokJenaConstants.NRFBO = relFragments.size();
        LothbrokJenaConstants.NRNBO = AbstractNode.getState().getNumRelevantNodes(relFragments);

        //mapping = AbstractNode.getState().getIndex().getMapping(triples);
        //long start = System.currentTimeMillis();
        CompatibilityGraph graph = ((IPartitionedIndex) AbstractNode.getState().getIndex()).getCompatibilityGraph(new ArrayList<>(stars.values()));
        //System.out.println(graph.toString());
        //System.out.println("Compatibility graph in " + (System.currentTimeMillis() - start) + " ms.");
        LothbrokJenaConstants.INDEXED = AbstractNode.getState().getIndex().getGraphs().size();
        LothbrokJenaConstants.LOCAL = AbstractNode.getState().getNumLocallyStored(graph.getFragments());

        IQueryStrategy strategy = ((IPartitionedIndex) AbstractNode.getState().getIndex()).getQueryStrategy(new ArrayList<>(stars.values()), graph);
        //System.out.println(strategy.toString());
        //System.out.println("Strategy in " + (System.currentTimeMillis() - start) + " ms.");
        Set<IGraph> fragments = strategy.getFragments();
        LothbrokJenaConstants.NRF = fragments.size();
        LothbrokJenaConstants.NRN = AbstractNode.getState().getNumRelevantNodes(fragments);

        Set<INeighborNode> involvedNodes = strategy.getInvolvedNodes();
        involvedNodes.add(AbstractNode.getState().getAsNeighborNode());
        LothbrokJenaConstants.NIQ = involvedNodes.size();
        LothbrokJenaConstants.NODES_INVOLVED.addAll(involvedNodes);

        //chain = new QueryIterLothbrok(chain, QueryStrategyFactory.buildEmptyStrategy(), execContext);
        chain = new QueryIterLothbrok(chain, strategy, execContext);
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
