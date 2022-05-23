package org.piqnic.piqnic.writer;

public class ResponseWriterFactory {
    public static IResponseWriter createWriter() {
        return new ResponseWriterHtml();
    }
}
