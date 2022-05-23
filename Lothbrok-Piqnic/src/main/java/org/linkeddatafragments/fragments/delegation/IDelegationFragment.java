package org.linkeddatafragments.fragments.delegation;

import org.apache.jena.rdf.model.Model;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.lothbrok.sparql.LothbrokBindings;

import java.util.List;

public interface IDelegationFragment extends ILinkedDataFragment {
    /**
     * Gets the total number of results in the fragment (can be an estimate).
     * @return the total number of triples
     */
    long getTotalSize();

    /**
     * Gets the bindings that are a result
     * @return the bindings
     */
    LothbrokBindings getBindings();

    /**
     * Gets the next page URL
     * @return the bindings
     */
    String getNextPageUrl();
}
