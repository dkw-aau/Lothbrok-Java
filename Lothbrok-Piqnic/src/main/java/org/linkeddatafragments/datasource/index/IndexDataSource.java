package org.linkeddatafragments.datasource.index;

import java.util.HashMap;
import java.util.Set;

import org.linkeddatafragments.datasource.DataSourceBase;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.fragments.tpf.TPFRequestParserForJenaBackends;
import org.lothbrok.index.ppbf.IBloomFilter;
import org.lothbrok.index.ppbf.IPartitionedBloomFilter;
import org.lothbrok.index.ppbf.impl.PrefixPartitionedBloomFilter;
import org.lothbrok.utils.Triple;
import org.rdfhdt.hdt.hdt.HDT;


public class IndexDataSource extends DataSourceBase {

    /**
     * The request processor
     *
     */
    protected final IndexRequestProcessorForTPFs requestProcessor;

    /**
     *
     * @param baseUrl
     * @param datasources
     */
    public IndexDataSource(String baseUrl, HashMap<String, IDataSource> datasources) {
        super("Index", "List of all datasources");
        requestProcessor = new IndexRequestProcessorForTPFs( baseUrl, datasources );
    }

    @Override
    public IFragmentRequestParser getRequestParser(IDataSource.ProcessorType processor)
    {
        return TPFRequestParserForJenaBackends.getInstance();
    }

    @Override
    public IPartitionedBloomFilter<String> createPartitionedBloomFilter() {
        return null;
    }

    @Override
    public IFragmentRequestProcessor getRequestProcessor(IDataSource.ProcessorType processor)
    {
        return requestProcessor;
    }

    @Override
    public void reload() {

    }

    @Override
    public int numTriples() {
        return 0;
    }

    @Override
    public int numSubjects() {
        return 0;
    }

    @Override
    public int numPredicates() {
        return 0;
    }

    @Override
    public int numObjects() {
        return 0;
    }

    @Override
    public void deleteBloomFilter() {

    }


    @Override
    public void copy() {

    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void restore() {

    }

    @Override
    public IDataSource materializeVersion(Set<Triple> additions, Set<Triple> deletions, long timestamp) {
        return null;
    }

    @Override
    public HDT getHdt() {
        return null;
    }

    @Override
    public IBloomFilter<String> createBloomFilter() {
        return PrefixPartitionedBloomFilter.create("empty.ppbf");
    }

    @Override
    public void remove() {

    }
}
