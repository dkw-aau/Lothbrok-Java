package org.linkeddatafragments.datasource;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.spf.IStarPatternElement;
import org.linkeddatafragments.fragments.spf.IStarPatternFragment;
import org.linkeddatafragments.fragments.spf.IStarPatternFragmentRequest;
import org.linkeddatafragments.fragments.spf.StarPatternFragmentImpl;
import org.lothbrok.utils.Tuple;

import java.util.List;

public abstract class AbstractRequestProcessorForStarPatterns<CTT,NVT,AVT>
        extends AbstractRequestProcessor {
    /**
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected final Worker<CTT,NVT,AVT> getWorker(
            final ILinkedDataFragmentRequest request )
            throws IllegalArgumentException
    {
        if ( request instanceof IStarPatternFragmentRequest<?,?,?>) {
            @SuppressWarnings("unchecked")
            final IStarPatternFragmentRequest<CTT,NVT,AVT> spfRequest =
                    (IStarPatternFragmentRequest<CTT,NVT,AVT>) request;
            return getSPFSpecificWorker( spfRequest );
        }
        else
            throw new IllegalArgumentException( request.getClass().getName() );
    }

    /**
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    abstract protected Worker<CTT,NVT,AVT> getSPFSpecificWorker(
            final IStarPatternFragmentRequest<CTT,NVT,AVT> request )
            throws IllegalArgumentException;

    /**
     *
     * @param <CTT>
     * @param <NVT>
     * @param <AVT>
     */
    abstract static protected class Worker<CTT,NVT,AVT>
            extends AbstractRequestProcessor.Worker
    {

        /**
         *
         * @param request
         */
        public Worker(
                final IStarPatternFragmentRequest<CTT,NVT,AVT> request )
        {
            super( request );
        }

        /**
         *
         * @return
         * @throws IllegalArgumentException
         */
        @Override
        public ILinkedDataFragment createRequestedFragment()
                throws IllegalArgumentException
        {
            final long limit = ILinkedDataFragmentRequest.TRIPLESPERPAGE;
            final long offset;
            if ( request.isPageRequest() )
                offset = limit * ( request.getPageNumber() - 1L );
            else
                offset = 0L;

            @SuppressWarnings("unchecked")
            final IStarPatternFragmentRequest<CTT,NVT,AVT> spfRequest =
                    (IStarPatternFragmentRequest<CTT,NVT,AVT>) request;

            return createFragment( spfRequest.getSubject(),
                    spfRequest.getStars(),
                    spfRequest.getBindings(),
                    offset, limit, spfRequest.getRequestHash() );
        }

        /**
         *
         * @param subj
         * @param stars
         * @param offset
         * @param bindings
         * @param limit
         * @return
         * @throws IllegalArgumentException
         */
        abstract protected ILinkedDataFragment createFragment(
                final IStarPatternElement<CTT,NVT,AVT> subj,
                final List<Tuple<IStarPatternElement<CTT,NVT,AVT>, IStarPatternElement<CTT,NVT,AVT>>> stars,
                final List<Binding> bindings,
                final long offset,
                final long limit,
                final long requestHash)
                throws IllegalArgumentException;

        /**
         *
         * @return
         */
        protected IStarPatternFragment createEmptyStarPatternFragment()
        {
            return new StarPatternFragmentImpl( request.getFragmentURL(),
                    request.getDatasetURL() );
        }

        /**
         *
         * @param triples
         * @param totalSize
         * @param isLastPage
         * @return
         */
        protected IStarPatternFragment createStarPatternFragment(
                final List<Model> triples,
                final long totalSize,
                final boolean isLastPage )
        {
            return new StarPatternFragmentImpl( triples,
                    totalSize,
                    request.getFragmentURL(),
                    request.getDatasetURL(),
                    request.getPageNumber(),
                    isLastPage );
        }

    } // end of class Worker
}
