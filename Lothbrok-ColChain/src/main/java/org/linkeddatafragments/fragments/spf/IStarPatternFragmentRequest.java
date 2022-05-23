package org.linkeddatafragments.fragments.spf;

import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.lothbrok.utils.Tuple;

import java.util.List;

public interface IStarPatternFragmentRequest<ConstantTermType,NamedVarType,AnonVarType>
        extends ILinkedDataFragmentRequest {
    /**
     * Returns the subject position of the requested star pattern.
     * @return
     */
    IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType> getSubject();

    /**
     * Returns the predicate in the correct position of the requested star pattern.
     * @return
     */
    IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType> getPredicate(int index);

    /**
     * Returns the object in the correct position of the requested star pattern.
     * @return
     */
    IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType> getObject(int index);

    List<Tuple<IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>,
                IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>>> getStars();

    /**
     * Returns the number of triples in the star pattern
     * @return
     */
    int getTriples();

    List<Binding> getBindings();

    long getRequestHash();
}
