package org.linkeddatafragments.datasource;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.delegation.DelegationFragmentImpl;
import org.linkeddatafragments.fragments.delegation.IDelegationFragment;
import org.linkeddatafragments.fragments.delegation.IDelegationFragmentRequest;
import org.linkeddatafragments.fragments.spf.IStarPatternElement;
import org.linkeddatafragments.fragments.spf.IStarPatternFragment;
import org.linkeddatafragments.fragments.spf.IStarPatternFragmentRequest;
import org.linkeddatafragments.fragments.spf.StarPatternFragmentImpl;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.utils.Tuple;

import java.util.List;

public abstract class AbstractRequestProcessorForDelegations extends AbstractRequestProcessor {
    /**
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected final Worker getWorker(
            final ILinkedDataFragmentRequest request )
            throws IllegalArgumentException
    {
        if ( request instanceof IDelegationFragmentRequest) {
            @SuppressWarnings("unchecked")
            final IDelegationFragmentRequest delRequest =
                    (IDelegationFragmentRequest) request;
            return getDelegationSpecificWorker(delRequest);
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
    abstract protected Worker getDelegationSpecificWorker(
            final IDelegationFragmentRequest request )
            throws IllegalArgumentException;


    abstract static protected class Worker
            extends AbstractRequestProcessor.Worker
    {

        /**
         *
         * @param request
         */
        public Worker(
                final IDelegationFragmentRequest request )
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
            final IDelegationFragmentRequest delRequest =
                    (IDelegationFragmentRequest) request;

            return createFragment(delRequest.getStrategy(),
                    delRequest.getBindings(),
                    offset, limit, delRequest.getRequestHash() );
        }

        /**
         *
         * @param strategy
         * @param offset
         * @param bindings
         * @param limit
         * @return
         * @throws IllegalArgumentException
         */
        abstract protected ILinkedDataFragment createFragment(
                final IQueryStrategy strategy,
                final List<Binding> bindings,
                final long offset,
                final long limit,
                final long requestHash)
                throws IllegalArgumentException;

        /**
         *
         * @return
         */
        protected IDelegationFragment createEmptyDelegationFragment()
        {
            return new DelegationFragmentImpl( request.getFragmentURL(),
                    request.getDatasetURL() );
        }

        /**
         *
         * @param bindings
         * @param totalSize
         * @param isLastPage
         * @return
         */
        protected IDelegationFragment createDelegationFragment(
                final LothbrokBindings bindings,
                final long totalSize,
                final boolean isLastPage )
        {
            return new DelegationFragmentImpl( bindings,
                    totalSize,
                    request.getFragmentURL(),
                    request.getDatasetURL(),
                    request.getPageNumber(),
                    isLastPage );
        }

    } // end of class Worker
}
