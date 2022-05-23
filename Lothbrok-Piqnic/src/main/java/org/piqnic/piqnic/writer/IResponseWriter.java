package org.piqnic.piqnic.writer;

import org.lothbrok.utils.Tuple;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import java.util.Set;


public interface IResponseWriter {
    /**
     * Serializes and writes not initiated message
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeNotInitiated(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    /**
     * Serializes and writes landing page
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeLandingPage(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    /**
     * Serializes and writes redirect
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeRedirect(ServletOutputStream outputStream, HttpServletRequest request, String path) throws Exception;

    /**
     * Serializes and writes query results
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeQueryResults(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    /**
     * Serializes and writes initial page
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeInit(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    void writeFragmentSearch(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    void writeFragmentDetails(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;
}
