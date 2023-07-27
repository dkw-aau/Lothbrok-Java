package org.lothbrok.writer;

import com.google.gson.Gson;
import com.google.gson.JsonPrimitive;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.delegation.IDelegationFragment;

import jakarta.servlet.ServletOutputStream;
import java.util.*;

public class LothbrokResponseWriterJson implements ILothbrokResponseWriter {
    private final Gson gson;
    protected LothbrokResponseWriterJson() {
        gson = new Gson();
    }

    @Override
    public void writeDelegationResults(ServletOutputStream outputStream, IDelegationFragment fragment) throws Exception {
        List<Map<String, String>> lst = new ArrayList<>();
        for(Binding b : fragment.getBindings().getBindings()) {
            Map<String, String> map = new HashMap<>();
            Iterator<Var> it = b.vars();
            while (it.hasNext()) {
                Var v = it.next();
                String varname = v.getVarName().startsWith("?") ? v.getVarName() : "?" + v.getVarName();
                Node n = b.get(v);
                String val;
                if (n.isLiteral()) val = n.getLiteralLexicalForm().replace("\n", " ");
                else if (n.isBlank()) val = n.getBlankNodeLabel();
                else val = n.getURI();

                map.put(varname, val);
            }
            lst.add(map);
        }
        String bindStr = this.gson.toJson(lst);
        String nextPageStr = fragment.isLastPage() ? "nil" : fragment.getNextPageUrl();

        outputStream.println(bindStr);
        outputStream.println(nextPageStr);
    }
}
