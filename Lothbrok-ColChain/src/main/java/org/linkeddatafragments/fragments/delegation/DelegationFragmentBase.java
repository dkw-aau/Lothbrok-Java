package org.linkeddatafragments.fragments.delegation;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.*;
import org.apache.jena.util.iterator.NiceIterator;
import org.linkeddatafragments.fragments.LinkedDataFragmentBase;
import org.linkeddatafragments.fragments.spf.IStarPatternFragment;
import org.linkeddatafragments.fragments.spf.StarPatternFragmentBase;
import org.linkeddatafragments.util.CommonResources;
import org.lothbrok.sparql.LothbrokBindings;

import java.util.List;
import java.util.NoSuchElementException;

import static com.github.jsonldjava.core.JsonLdConsts.XSD_INTEGER;
import static com.github.jsonldjava.core.JsonLdConsts.XSD_STRING;
import static org.linkeddatafragments.util.CommonResources.*;
import static org.linkeddatafragments.util.CommonResources.HYDRA_PROPERTY;

public abstract class DelegationFragmentBase extends LinkedDataFragmentBase
        implements IDelegationFragment {
    private final long totalSize;

    /**
     * Creates an empty Triple Pattern Fragment.
     * @param fragmentURL
     * @param datasetURL
     */
    public DelegationFragmentBase( final String fragmentURL,
                                    final String datasetURL ) {
        this( 0L, fragmentURL, datasetURL, 1, true );
    }

    /**
     * Creates an empty Triple Pattern Fragment page.
     * @param fragmentURL
     * @param isLastPage
     * @param datasetURL
     * @param pageNumber
     */
    public DelegationFragmentBase( final String fragmentURL,
                                    final String datasetURL,
                                    final long pageNumber,
                                    final boolean isLastPage ) {
        this( 0L, fragmentURL, datasetURL, pageNumber, isLastPage );
    }

    /**
     * Creates a new Triple Pattern Fragment.
     * @param totalSize the total size
     * @param fragmentURL
     * @param datasetURL
     * @param pageNumber
     * @param isLastPage
     */
    public DelegationFragmentBase( long totalSize,
                                    final String fragmentURL,
                                    final String datasetURL,
                                    final long pageNumber,
                                    final boolean isLastPage ) {
        super( fragmentURL, datasetURL, pageNumber, isLastPage );
        this.totalSize = totalSize < 0L ? 0L : totalSize;
    }

    public DelegationFragmentBase( long totalSize,
                                    final String fragmentURL,
                                    final String datasetURL,
                                    final long pageNumber,
                                    final boolean isLastPage,
                                    final int numResults) {
        super( fragmentURL, datasetURL, pageNumber, isLastPage, numResults );
        this.totalSize = totalSize < 0L ? 0L : totalSize;
    }

    @Override
    public String getNextPageUrl() {
        return this.fragmentURL + "&page=" + (this.pageNumber + 1);
    }
}
