package org.linkeddatafragments.fragments.delegation;

import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.LinkedDataFragmentRequestBase;
import org.linkeddatafragments.fragments.spf.IStarPatternElement;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.utils.Tuple;

import java.util.List;

public class DelegationFragmentRequestImpl extends LinkedDataFragmentRequestBase
        implements IDelegationFragmentRequest {

    public final List<Binding> bindings;

    private final long requestHash;

    private final IQueryStrategy strategy;

    public DelegationFragmentRequestImpl(final String fragmentURL,
                                         final String datasetURL,
                                         final boolean pageNumberWasRequested,
                                         final long pageNumber,
                                         final List<Binding> bindings,
                                         final IQueryStrategy strategy,
                                         final long requestHash
    ) {
        super(fragmentURL, datasetURL, pageNumberWasRequested, pageNumber);

        this.bindings = bindings;
        this.requestHash = requestHash;
        this.strategy = strategy;
    }

    @Override
    public IQueryStrategy getStrategy() {
        return strategy;
    }

    @Override
    public List<Binding> getBindings() {
        return bindings;
    }

    @Override
    public long getRequestHash() {
        return requestHash;
    }
}
