package com.diplome.shared.enums;

import org.springframework.context.annotation.Bean;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseDrivers {

    public static final Map<String, String> databaseDrivers = databaseDrivers();

    private static Map<String, String> databaseDrivers(){
        Map<String, String> map = new ConcurrentHashMap<>();
        map.put("MYSQL", "com.mysql.jdbc.Driver");
        map.put("POSTGRES", "org.postgresql.Driver");
        map.put("SQLSERVER", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return Collections.unmodifiableMap(map);
    }
}
