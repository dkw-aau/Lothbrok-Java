package org.lothbrok.writer;

import com.google.gson.Gson;
import org.linkeddatafragments.fragments.delegation.IDelegationFragment;

import javax.servlet.ServletOutputStream;

public class LothbrokResponseWriterJson implements ILothbrokResponseWriter {
    private final Gson gson;
    protected LothbrokResponseWriterJson() {
        gson = new Gson();
    }

    @Override
    public void writeDelegationResults(ServletOutputStream outputStream, IDelegationFragment fragment) throws Exception {
        String bindStr = this.gson.toJson(fragment.getBindings().getBindings());
        String nextPageStr = fragment.isLastPage() ? "nil" : fragment.getNextPageUrl();

        outputStream.println(bindStr);
        outputStream.println(nextPageStr);
    }
}
