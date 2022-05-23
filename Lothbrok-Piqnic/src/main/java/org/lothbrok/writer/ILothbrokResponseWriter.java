package org.lothbrok.writer;

import org.linkeddatafragments.fragments.delegation.IDelegationFragment;

import javax.servlet.ServletOutputStream;
import java.io.IOException;

public interface ILothbrokResponseWriter {

    /**
     * Serializes and writes delegation results
     *
     * @param outputStream The response stream to write to
     * @param fragment Delegation fragment results
     * @throws Exception Error that occurs while serializing
     */
    void writeDelegationResults(ServletOutputStream outputStream, IDelegationFragment fragment) throws Exception;
}
