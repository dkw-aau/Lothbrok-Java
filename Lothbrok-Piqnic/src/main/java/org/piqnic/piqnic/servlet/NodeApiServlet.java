package org.piqnic.piqnic.servlet;

import org.lothbrok.utils.FragmentUtils;
import org.piqnic.piqnic.node.AbstractNode;
import org.piqnic.piqnic.writer.IResponseWriter;
import org.piqnic.piqnic.writer.ResponseWriterFactory;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.rdfhdt.hdt.exceptions.NotImplementedException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.List;

public class NodeApiServlet extends HttpServlet {
    private IResponseWriter writer = ResponseWriterFactory.createWriter();
    private final CloseableHttpClient httpClient = HttpClients.createDefault();

    /**
     * @param servletConfig
     * @throws ServletException
     */
    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
    }

    /**
     *
     */
    @Override
    public void destroy() {
    }


    /**
     * @param request
     * @param response
     * @throws ServletException
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (!WebInterfaceServlet.INIT) {
            try {
                writer.writeNotInitiated(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        }

        String contextPath = request.getContextPath();
        String requestURI = request.getRequestURI();

        String path = contextPath == null
                ? requestURI
                : requestURI.substring(contextPath.length());

        String endpoint = path.substring(path.lastIndexOf("/") + 1);

        switch (endpoint) {
            case "upload":
                handleUpload(request, response);
                break;
            case "search":
                handleSearch(request, response);
                break;
            case "fragment":
                handleFragment(request, response);
                break;
            case "neighbor":
                handleNeighbor(request, response);
                break;
            case "sparql":
                handleSparql(request, response);
                break;
            default:
                response.getWriter().println("Unknown request.");
        }
    }

    private void handleSparql(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            writer.writeQueryResults(response.getOutputStream(), request);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void handleUpload(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        String path = request.getParameter("file");

        FragmentUtils.upload(path);

        try {
            writer.writeRedirect(response.getOutputStream(), request, "api/upload");
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void handleSearch(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        try {
            writer.writeFragmentSearch(response.getOutputStream(), request);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void handleFragment(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        try {
            writer.writeFragmentDetails(response.getOutputStream(), request);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void handleNeighbor(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        //ToDo: Implement this.
        throw new NotImplementedException("It is not possible to add neighbors in the prototype.");
    }
}
