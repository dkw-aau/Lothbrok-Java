package org.lothbrok.writer;

public class LothbrokResponseWriterFactory {
    public static ILothbrokResponseWriter create() {
        return new LothbrokResponseWriterJson();
    }
}
