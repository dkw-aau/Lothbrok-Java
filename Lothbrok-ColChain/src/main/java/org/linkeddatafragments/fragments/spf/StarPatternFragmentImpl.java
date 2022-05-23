package org.linkeddatafragments.fragments.spf;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.StmtIterator;

import java.util.ArrayList;
import java.util.List;

public class StarPatternFragmentImpl extends StarPatternFragmentBase {
    /**
     *
     */
    protected final List<Model> triples;

    /**
     * Creates an empty Triple Pattern Fragment.
     * @param fragmentURL
     * @param datasetURL
     */
    public StarPatternFragmentImpl( final String fragmentURL,
                                    final String datasetURL ) {
        this( new ArrayList<>(), 0L, fragmentURL, datasetURL, 1, true );
    }

    public StarPatternFragmentImpl( final String fragmentURL,
                                    final String datasetURL,
                                    final int numResults ) {
        this( new ArrayList<>(), 0L, fragmentURL, datasetURL, 1, true, numResults );
    }

    /**
     * Creates an empty Triple Pattern Fragment page.
     * @param fragmentURL
     * @param datasetURL
     * @param isLastPage
     * @param pageNumber
     */
    public StarPatternFragmentImpl( final String fragmentURL,
                                    final String datasetURL,
                                    final long pageNumber,
                                    final boolean isLastPage ) {
        this( new ArrayList<>(), 0L, fragmentURL, datasetURL, pageNumber, isLastPage );
    }

    public StarPatternFragmentImpl( final String fragmentURL,
                                    final String datasetURL,
                                    final long pageNumber,
                                    final boolean isLastPage,
                                    final int numResults ) {
        this( new ArrayList<>(), 0L, fragmentURL, datasetURL, pageNumber, isLastPage , numResults);
    }

    /**
     * Creates a new Triple Pattern Fragment.
     * @param triples the triples (possibly partial)
     * @param totalSize the total size
     * @param fragmentURL
     * @param datasetURL
     * @param isLastPage
     * @param pageNumber
     */
    public StarPatternFragmentImpl( final List<Model> triples,
                                    long totalSize,
                                    final String fragmentURL,
                                    final String datasetURL,
                                    final long pageNumber,
                                    final boolean isLastPage ) {
        super( totalSize, fragmentURL, datasetURL, pageNumber, isLastPage );
        this.triples = triples;
    }

    public StarPatternFragmentImpl( final List<Model> triples,
                                    long totalSize,
                                    final String fragmentURL,
                                    final String datasetURL,
                                    final long pageNumber,
                                    final boolean isLastPage,
                                    final int numResults ) {
        super( totalSize, fragmentURL, datasetURL, pageNumber, isLastPage, numResults );
        this.triples = triples;
    }

    /**
     *
     * @return
     */
    @Override
    protected List<StmtIterator> getNonEmptyStmtIterators() {
        List<StmtIterator> lst = new ArrayList<>();

        for(Model m : triples) {
            lst.add(m.listStatements());
        }

        return lst;
    }

    @Override
    public List<Model> getModels() {
        return triples;
    }
}
