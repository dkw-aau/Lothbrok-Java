package org.linkeddatafragments.fragments.delegation;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.piqnic.piqnic.node.AbstractNode;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.fragments.FragmentRequestParserBase;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.lothbrok.compatibilitygraph.CompatibilityGraph;
import org.lothbrok.index.index.IPartitionedIndex;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.utils.Tuple;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class DelegationRequestParser
        extends FragmentRequestParserBase {

    /**
     *
     * @param httpRequest
     * @param config
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected Worker getWorker(final HttpServletRequest httpRequest,
                                                final ConfigReader config )
            throws IllegalArgumentException
    {
        return new Worker( httpRequest, config );
    }

    /**
     *
     */
    protected class Worker extends FragmentRequestParserBase.Worker
    {

        /**
         *
         * @param request
         * @param config
         */
        public Worker( final HttpServletRequest request,
                       final ConfigReader config )
        {
            super( request, config );
        }

        /**
         *
         * @return
         * @throws IllegalArgumentException
         */
        @Override
        public ILinkedDataFragmentRequest createFragmentRequest()
                throws IllegalArgumentException
        {
            // System.out.println("Create Fragment Request :)");
            return new DelegationFragmentRequestImpl(
                    getFragmentURL(),
                    getDatasetURL(),
                    pageNumberWasRequested,
                    pageNumber,
                    getBindings(),
                    getStrategy(),
                    getRequestHash());
        }

        private long getRequestHash() {
            String s = request.getParameter("s");
            long sHash = 0;
            if(s != null)
                sHash = s.hashCode();

            String star = request.getParameter("star");
            long starHash = 0;
            if(star != null)
                starHash = star.hashCode();

            String bindings = request.getParameter("values");
            long bindingsHash = 0;
            if(bindings != null)
                bindingsHash = bindings.hashCode();

            return sHash + starHash + bindingsHash;
        }

        public List<Binding> getBindings() {
            final List<Var> foundVariables = new ArrayList<Var>();
            return parseAsSetOfBindings(
                    request.getParameter("values"),
                    foundVariables);
        }

        private IQueryStrategy getStrategy() {
            Gson gson = new Gson();
            Type type = new TypeToken<List<StarStringTmp>>() { }.getType();
            List<StarStringTmp> bgpTmp = gson.fromJson(request.getParameter("query"), type);
            List<StarString> bgp = new ArrayList<>();
            for(StarStringTmp tmp : bgpTmp) bgp.add(tmp.getAsStarString());

            CompatibilityGraph graph = ((IPartitionedIndex)AbstractNode.getState().getIndex()).getCompatibilityGraph(bgp);
            return ((IPartitionedIndex)AbstractNode.getState().getIndex()).getQueryStrategy(bgp, graph);
        }

        /**
         * Parses the given value as set of bindings.
         *
         * @param value          containing the SPARQL bindings
         * @param foundVariables a list with variables found in the VALUES clause
         * @return a list with solution mappings found in the VALUES clause
         */
        private List<Binding> parseAsSetOfBindings(final String value, final List<Var> foundVariables) {
            if (value == null) {
                return null;
            }
            String newString = "select * where {} VALUES " + value;
            Query q = QueryFactory.create(newString);
            foundVariables.addAll(q.getValuesVariables());
            return q.getValuesData();
        }

    } // end of class Worker

    protected class StarStringTmp {
        private final String subject;
        private final List<Tuple<String, String>> triples;

        public StarStringTmp(String subject, List<Tuple<String, String>> triples) {
            this.subject = subject;
            this.triples = triples;
        }

        protected StarString getAsStarString() {
            List<Tuple<CharSequence, CharSequence>> lst = new ArrayList<>();
            for(Tuple<String, String> t : triples) {
                lst.add(new Tuple<>(t.x, t.y));
            }
            return new StarString(subject, lst);
        }
    }
}
