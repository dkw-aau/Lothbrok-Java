package org.linkeddatafragments.fragments.delegation;

import org.linkeddatafragments.fragments.spf.SPFRequestParserForJenaBackends;
import org.linkeddatafragments.util.StarPatternElementParserForJena;

public class DelegationRequestParserForJenaBackends extends DelegationRequestParser {
    private static DelegationRequestParserForJenaBackends instance = null;

    /**
     *
     * @return
     */
    public static DelegationRequestParserForJenaBackends getInstance()
    {
        if ( instance == null ) {
            instance = new DelegationRequestParserForJenaBackends();
        }
        return instance;
    }
}
