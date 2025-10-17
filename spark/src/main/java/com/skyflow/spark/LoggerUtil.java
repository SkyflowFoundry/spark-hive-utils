package com.skyflow.spark;

import java.util.logging.Logger;

public class LoggerUtil {
    public static Logger getLogger(Class<?> clazz) {
        return Logger.getLogger(clazz.getName());
    }
}
