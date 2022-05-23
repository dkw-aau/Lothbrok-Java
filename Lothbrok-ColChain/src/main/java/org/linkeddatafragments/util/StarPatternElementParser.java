package org.linkeddatafragments.util;

import org.linkeddatafragments.fragments.spf.IStarPatternElement;
import org.linkeddatafragments.fragments.spf.StarPatternElementFactory;

public abstract class StarPatternElementParser<ConstantTermType,NamedVarType,AnonVarType>
        extends RDFTermParser<ConstantTermType> {
    /**
     *
     */
    public final StarPatternElementFactory<ConstantTermType,NamedVarType,AnonVarType>
            factory = new StarPatternElementFactory<ConstantTermType,NamedVarType,AnonVarType>();

    /**
     *
     * @param param
     * @return
     */
    public IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>
    parseIntoStarPatternElement( final String param )
    {
        // nothing or empty indicates an unspecified variable
        if ( param == null || param.isEmpty() )
            return factory.createUnspecifiedVariable();

        // identify the kind of RDF term based on the first character
        char firstChar = param.charAt(0);
        switch ( firstChar )
        {
            // specific variable that has a name
            case '?':
            {
                final String varName = param.substring(1);
                final NamedVarType var = createNamedVariable( varName );
                return factory.createNamedVariable( var );
            }

            // specific variable that is denoted by a blank node
            case '_':
            {
                final AnonVarType var = createAnonymousVariable( param );
                return factory.createAnonymousVariable( var );
            }

            // assume it is an RDF term
            default:
                return factory.createConstantRDFTerm( parseIntoRDFNode(param) );
        }
    }

    /**
     *
     * @param varName
     * @return
     */
    abstract public NamedVarType createNamedVariable( final String varName );

    /**
     *
     * @param label
     * @return
     */
    abstract public AnonVarType createAnonymousVariable( final String label );
}
