package org.linkeddatafragments.fragments.delegation;

import org.apache.jena.rdf.model.StmtIterator;
import org.lothbrok.sparql.LothbrokBindings;
import org.rdfhdt.hdt.exceptions.NotImplementedException;


public class DelegationFragmentImpl extends DelegationFragmentBase {
    /**
     *
     */
    protected final LothbrokBindings bindings;

    /**
     * Creates an empty Triple Pattern Fragment.
     * @param fragmentURL
     * @param datasetURL
     */
    public DelegationFragmentImpl( final String fragmentURL,
                                    final String datasetURL ) {
        this( new LothbrokBindings(), 0L, fragmentURL, datasetURL, 1, true );
    }

    public DelegationFragmentImpl( final String fragmentURL,
                                    final String datasetURL,
                                    final int numResults ) {
        this( new LothbrokBindings(), 0L, fragmentURL, datasetURL, 1, true, numResults );
    }

    /**
     * Creates an empty Triple Pattern Fragment page.
     * @param fragmentURL
     * @param datasetURL
     * @param isLastPage
     * @param pageNumber
     */
    public DelegationFragmentImpl( final String fragmentURL,
                                    final String datasetURL,
                                    final long pageNumber,
                                    final boolean isLastPage ) {
        this( new LothbrokBindings(), 0L, fragmentURL, datasetURL, pageNumber, isLastPage );
    }

    public DelegationFragmentImpl( final String fragmentURL,
                                    final String datasetURL,
                                    final long pageNumber,
                                    final boolean isLastPage,
                                    final int numResults ) {
        this( new LothbrokBindings(), 0L, fragmentURL, datasetURL, pageNumber, isLastPage , numResults);
    }

    /**
     * Creates a new Triple Pattern Fragment.
     * @param bindings the bindings (possibly partial)
     * @param totalSize the total size
     * @param fragmentURL
     * @param datasetURL
     * @param isLastPage
     * @param pageNumber
     */
    public DelegationFragmentImpl( final LothbrokBindings bindings,
                                    long totalSize,
                                    final String fragmentURL,
                                    final String datasetURL,
                                    final long pageNumber,
                                    final boolean isLastPage ) {
        super( totalSize, fragmentURL, datasetURL, pageNumber, isLastPage );
        this.bindings = bindings;
    }

    /**
     * Creates a new Triple Pattern Fragment.
     * @param bindings the bindings (possibly partial)
     * @param totalSize the total size
     * @param fragmentURL
     * @param datasetURL
     * @param pageNumber
     * @param isLastPage
     * @param numResults
     */
    public DelegationFragmentImpl( final LothbrokBindings bindings,
                                    long totalSize,
                                    final String fragmentURL,
                                    final String datasetURL,
                                    final long pageNumber,
                                    final boolean isLastPage,
                                    final int numResults ) {
        super( totalSize, fragmentURL, datasetURL, pageNumber, isLastPage, numResults );
        this.bindings = bindings;
    }

    @Override
    public LothbrokBindings getBindings() {
        return bindings;
    }

    @Override
    public StmtIterator getTriples() {
        throw new NotImplementedException("Not implemented for delegation fragment");
    }

    @Override
    public long getTotalSize() {
        return bindings.size();
    }
}
