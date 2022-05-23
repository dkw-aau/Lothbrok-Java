package org.linkeddatafragments.fragments.delegation;

import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.lothbrok.strategy.IQueryStrategy;

import java.util.List;

public interface IDelegationFragmentRequest extends ILinkedDataFragmentRequest {
    IQueryStrategy getStrategy();

    List<Binding> getBindings();

    long getRequestHash();
}
