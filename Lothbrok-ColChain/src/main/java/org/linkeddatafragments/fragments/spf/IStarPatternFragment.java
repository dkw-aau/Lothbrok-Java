package org.linkeddatafragments.fragments.spf;

import org.apache.jena.rdf.model.Model;
import org.linkeddatafragments.fragments.ILinkedDataFragment;

import java.util.List;

public interface IStarPatternFragment  extends ILinkedDataFragment {
    /**
     * Gets the total number of triples in the fragment (can be an estimate).
     * @return the total number of triples
     */
    public long getTotalSize();

    /**
     * Gets the total number of triples in the fragment (can be an estimate).
     * @return the total number of triples
     */
    public List<Model> getModels();
}
