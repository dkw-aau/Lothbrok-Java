package org.lothbrok.servlet;

import org.apache.http.HttpHeaders;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.riot.Lang;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.servlet.WebInterfaceServlet;
import org.colchain.colchain.writer.ResponseWriterFactory;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.delegate.DelegateDataSource;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.delegation.DelegationFragmentImpl;
import org.linkeddatafragments.fragments.delegation.IDelegationFragment;
import org.linkeddatafragments.util.MIMEParse;
import org.linkeddatafragments.views.ILinkedDataFragmentWriter;
import org.linkeddatafragments.views.LinkedDataFragmentWriterFactory;
import org.lothbrok.writer.ILothbrokResponseWriter;
import org.lothbrok.writer.LothbrokResponseWriterFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class DelegationServlet extends HttpServlet {
    private ILothbrokResponseWriter writer = LothbrokResponseWriterFactory.create();
    public final static String CFGFILE = "configFile";
    private ConfigReader config;

    private File getConfigFile(ServletConfig config) throws IOException {
        String path = config.getServletContext().getRealPath("/");


        if (path == null) {
            path = System.getProperty("user.dir");
        }
        File cfg = new File("config-example.json");
        if (config.getInitParameter(CFGFILE) != null) {
            cfg = new File(config.getInitParameter(CFGFILE));
        }
        if (!cfg.exists()) {
            throw new IOException("Configuration file " + cfg + " not found.");
        }
        if (!cfg.isFile()) {
            throw new IOException("Configuration file " + cfg + " is not a file.");
        }
        return cfg;
    }

    /**
     * @param servletConfig
     * @throws ServletException
     */
    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        try {
            // load the configuration
            File configFile = getConfigFile(servletConfig);
            config = new ConfigReader(new FileReader(configFile));
            MIMEParse.register(Lang.TTL.getHeaderString());
        } catch (Exception e) {
            throw new ServletException(e);
        }
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
                ResponseWriterFactory.createWriter().writeNotInitiated(response.getOutputStream(), request);
            } catch (Exception e) {
                throw new ServletException(e);
            }
            return;
        }

        try {
            IDataSource dataSource = new DelegateDataSource();
            final ILinkedDataFragmentRequest ldfRequest = dataSource.getRequestParser(IDataSource.ProcessorType.DELEGATE)
                    .parseIntoFragmentRequest(request, config);
            final IDelegationFragment fragment = (DelegationFragmentImpl) dataSource.getRequestProcessor(IDataSource.ProcessorType.DELEGATE)
                    .createRequestedFragment(ldfRequest);
            writer.writeDelegationResults(response.getOutputStream(), fragment);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ServletException(e);
        }
    }
}
