package org.linkeddatafragments.fragments.spf;

import org.apache.jena.rdf.model.RDFNode;
import org.linkeddatafragments.util.StarPatternElementParserForJena;

public class SPFRequestParserForJenaBackends extends SPFRequestParser<RDFNode,String,String> {
    private static SPFRequestParserForJenaBackends instance = null;

    /**
     *
     * @return
     */
    public static SPFRequestParserForJenaBackends getInstance()
    {
        if ( instance == null ) {
            instance = new SPFRequestParserForJenaBackends();
        }
        return instance;
    }

    /**
     *
     */
    protected SPFRequestParserForJenaBackends()
    {
        super( StarPatternElementParserForJena.getInstance() );
    }
}
