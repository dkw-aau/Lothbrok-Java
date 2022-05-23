package org.colchain.colchain.writer;

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
     * Serializes and writes search poage
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeSearch(ServletOutputStream outputStream, HttpServletRequest request, Set<Tuple<String, Tuple<String, String>>> comms) throws Exception;

    /**
     * Serializes and writes initial page
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeInit(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    /**
     * Serializes and writes updates
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeSuggestUpdate(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    /**
     * Serializes and writes suggested update
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeSuggestedUpdate(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    /**
     * Serializes and writes community details
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeCommunityDetails(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    /**
     * Serializes and writes fragment details
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeFragmentDetails(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    /**
     * Serializes and writes transaction details
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeTransactionDetails(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    /**
     * Serializes and writes fragment search page
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeFragmentSearch(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;
}
