package org.linkeddatafragments.fragments.spf;

import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.LinkedDataFragmentRequestBase;
import org.lothbrok.utils.Tuple;

import java.util.List;

public class StarPatternFragmentRequestImpl<CTT,NVT,AVT>
        extends LinkedDataFragmentRequestBase
        implements IStarPatternFragmentRequest<CTT,NVT,AVT> {
    /**
     *
     */
    public final IStarPatternElement<CTT,NVT,AVT> subject;

    public final List<Tuple<IStarPatternElement<CTT,NVT,AVT>, IStarPatternElement<CTT,NVT,AVT>>> stars;

    public final List<Binding> bindings;

    private final int numTriples;

    private final long requestHash;

    /**
     *
     * @param fragmentURL
     * @param datasetURL
     * @param pageNumberWasRequested
     * @param pageNumber
     * @param subject
     */
    public StarPatternFragmentRequestImpl( final String fragmentURL,
                                           final String datasetURL,
                                           final boolean pageNumberWasRequested,
                                           final long pageNumber,
                                           final IStarPatternElement<CTT,NVT,AVT> subject,
                                           final List<Tuple<IStarPatternElement<CTT,NVT,AVT>, IStarPatternElement<CTT,NVT,AVT>>> stars,
                                           final List<Binding> bindings,
                                           final int triples,
                                           final long requestHash
    )
    {
        super( fragmentURL, datasetURL, pageNumberWasRequested, pageNumber );

        if ( subject == null )
            throw new IllegalArgumentException();

        if ( stars == null )
            throw new IllegalArgumentException();

        this.subject = subject;
        this.stars = stars;
        this.bindings = bindings;
        this.numTriples = triples;
        this.requestHash = requestHash;
    }

    @Override
    public long getRequestHash() {
        return requestHash;
    }

    @Override
    public IStarPatternElement<CTT,NVT,AVT> getSubject() {
        return subject;
    }

    @Override
    public IStarPatternElement<CTT,NVT,AVT> getPredicate(int index) {
        return stars.get(index).x;
    }

    @Override
    public IStarPatternElement<CTT,NVT,AVT> getObject(int index) {
        return stars.get(index).y;
    }

    @Override
    public List<Tuple<IStarPatternElement<CTT, NVT, AVT>, IStarPatternElement<CTT, NVT, AVT>>> getStars() {
        return stars;
    }

    @Override
    public List<Binding> getBindings() {
        return bindings;
    }

    @Override
    public int getTriples() {
        return numTriples;
    }

    @Override
    public String toString()
    {
        return "TriplePatternFragmentRequest(" +
                "class: " + getClass().getName() +
                ", subject: " + subject.toString() +
                ", stars: " + stars.toString() +
                ", fragmentURL: " + fragmentURL +
                ", isPageRequest: " + pageNumberWasRequested +
                ", pageNumber: " + pageNumber +
                ")";
    }
}
