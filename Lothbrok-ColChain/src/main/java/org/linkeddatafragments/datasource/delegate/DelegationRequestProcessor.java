package org.linkeddatafragments.datasource.delegate;

import org.apache.jena.sparql.engine.binding.Binding;
import org.colchain.colchain.node.AbstractNode;
import org.linkeddatafragments.datasource.AbstractRequestProcessorForDelegations;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.delegation.DelegationFragmentImpl;
import org.linkeddatafragments.fragments.delegation.IDelegationFragmentRequest;
import org.lothbrok.index.index.IPartitionedIndex;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.util.LRUCache;

import java.util.List;

public class DelegationRequestProcessor extends AbstractRequestProcessorForDelegations {
    private final String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    private final LRUCache<Tuple<Long, Long>, IQueryStrategy> pageCache;

    private final LothbrokGraph graph;

    public DelegationRequestProcessor(LRUCache<Tuple<Long, Long>, IQueryStrategy> pageCache, LothbrokGraph graph) {
        this.pageCache = pageCache;
        this.graph = graph;
    }

    /**
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected Worker getDelegationSpecificWorker(
            final IDelegationFragmentRequest request)
            throws IllegalArgumentException {
        return new Worker(request);
    }

    /**
     * Worker class for HDT
     */
    protected class Worker
            extends AbstractRequestProcessorForDelegations.Worker {


        /**
         * Create HDT Worker
         *
         * @param req
         */
        public Worker(
                final IDelegationFragmentRequest req) {
            super(req);
        }

        /**
         * Creates an {@link ILinkedDataFragment} from the HDT
         *
         * @param strategy
         * @param bindings
         * @param offset
         * @param limit
         * @return
         */
        @Override
        protected ILinkedDataFragment createFragment(
                final IQueryStrategy strategy,
                final List<Binding> bindings,
                final long offset,
                final long limit,
                final long requestHash) {
            final LothbrokBindings binds = new LothbrokBindings();
            Tuple<Long, Long> initialKey = new Tuple<>(requestHash, offset);
            LothbrokBindings bs = new LothbrokBindings();
            if(bindings != null) {
                for (Binding b : bindings) bs.add(b);
            }

            int found = 0;
            int skipped = 0;
            boolean isNew = true;
            IQueryStrategy strat;
            if (pageCache.containsKey(initialKey)) {
                strat = pageCache.get(initialKey);
                isNew = false;
            } else {
                strat = strategy;
            }

            final boolean hasMatches = strat.hasNextBinding(graph, bs);

            if(hasMatches) {
                boolean atOffset;

                if (isNew) {
                    for (int i = skipped; !(atOffset = i == offset)
                            && strat.hasNextBinding(graph, bs); i++) {
                        strat.moveToNextBinding(graph, bs);
                        skipped++;
                    }
                } else {
                    atOffset = true;
                }

                if (atOffset) {
                    for (int i = found; i < limit && strat.hasNextBinding(graph, bs); i++)
                        binds.add(strat.moveToNextBinding(graph, bs));
                }
            }

            boolean isLastPage = found < limit;

            if(!isLastPage && strat.hasNextBinding(graph, bs)) pageCache.put(initialKey, strat);
            return new DelegationFragmentImpl(binds, strategy.estimateCardinality((IPartitionedIndex) AbstractNode.getState().getIndex()),
                    request.getFragmentURL(), request.getDatasetURL(), request.getPageNumber(), isLastPage, found);
        }
    } // end of Worker
}
