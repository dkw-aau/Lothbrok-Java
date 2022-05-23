package org.linkeddatafragments.datasource.delegate;

import org.linkeddatafragments.datasource.DataSourceBase;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.fragments.delegation.DelegationRequestParserForJenaBackends;
import org.lothbrok.index.ppbf.IBloomFilter;
import org.lothbrok.index.ppbf.IPartitionedBloomFilter;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.utils.Triple;
import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.util.LRUCache;

import java.util.Set;

public class DelegateDataSource extends DataSourceBase {
    private final LothbrokGraph graph;
    protected static LRUCache<Tuple<Long, Long>, IQueryStrategy> pageCache = new LRUCache<>();

    public DelegateDataSource(String title, String description) {
        super(title, description);
        this.graph = new LothbrokGraph();
    }

    public DelegateDataSource() {
        super("delegate", "Delegate source");
        this.graph = new LothbrokGraph();
    }

    public DelegateDataSource(long timestamp) {
        super("delegate", "Delegate source");
        this.graph = new LothbrokGraph(timestamp);
    }

    public DelegateDataSource(String title, String description, long timestamp) {
        super(title, description);
        this.graph = new LothbrokGraph(timestamp);
    }

    @Override
    public IFragmentRequestParser getRequestParser(ProcessorType processor) {
        return DelegationRequestParserForJenaBackends.getInstance();
    }

    @Override
    public IFragmentRequestProcessor getRequestProcessor(ProcessorType processor) {
        return new DelegationRequestProcessor(pageCache, graph);
    }

    @Override
    public HDT getHdt() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public IBloomFilter<String> createBloomFilter() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public IPartitionedBloomFilter<String> createPartitionedBloomFilter() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public void remove() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public void deleteBloomFilter() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public void copy() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public long size() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public void restore() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public IDataSource materializeVersion(Set<Triple> additions, Set<Triple> deletions, long timestamp) {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public int numTriples() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public int numSubjects() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public int numPredicates() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public int numObjects() {
        throw new NotImplementedException("Not implemented for delegations.");
    }

    @Override
    public void reload() {
        throw new NotImplementedException("Not implemented for delegations.");
    }
}
