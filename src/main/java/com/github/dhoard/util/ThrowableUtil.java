package com.github.dhoard.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ThrowableUtil {

    public static String toString(Throwable t) {
        if (null == t) {
            return null;
        }

        StringWriter stringWriter = new StringWriter();

        PrintWriter printWriter = new PrintWriter(stringWriter);
        t.printStackTrace(printWriter);

        return stringWriter.toString();
    }

    public static void throwUnchecked(Throwable t) {
        if (t instanceof RuntimeException) {
            throw ((RuntimeException) t);
        } else {
            throw new RuntimeException(t);
        }
    }
}
