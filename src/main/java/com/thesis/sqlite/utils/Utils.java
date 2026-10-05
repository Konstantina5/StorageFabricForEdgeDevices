package com.thesis.sqlite.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
    public static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
    public static final String DISCOVERY_NODE_NAME = System.getenv("DISCOVERY");
    public static final String HOSTNAME = System.getenv("HOSTNAME");
    public static final String TABLE_NAME = System.getenv("DB_NAME");
    public static final String BASE_URL = System.getenv("BASE_URL");
    public static final String STREAMING_KEY = System.getenv("STREAMING_KEY");
}
