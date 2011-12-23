package ru.compscicenter.db.tolmachev.nosqldb.server.dbmanager;

import ru.compscicenter.db.tolmachev.nosqldb.server.database.Row;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class CommandLogger {

    private String name;

    private String logFilePath;

    private BufferedWriter printWriter;

    public CommandLogger(String logFilePath) throws IOException {
        this.logFilePath = logFilePath;
        printWriter = new BufferedWriter(new FileWriter(logFilePath, true));
    }

    public CommandLogger(String name, String logFilePath) throws IOException {
        this.name = name;
        this.logFilePath = logFilePath;
        this.printWriter = new BufferedWriter(new FileWriter(logFilePath, true));
    }


    public void log(String string) {
        try {
            printWriter = new BufferedWriter(new FileWriter(logFilePath, true));
            String time = getCurrentTime();
            String stringToLog = String.format("[%s] %s\n", time, string);
            printWriter.write(stringToLog);
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void log(String command, Collection<String> list) {
        StringBuffer sb = new StringBuffer();
        sb.append(command);
        sb.append(" (");
        Iterator<String> iterator = list.iterator();
        while (iterator.hasNext()) {
            sb.append(iterator.next());
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(") ");
        log(sb.toString());
    }

    public void log(String command, Map<String, Row> map) {

        for (String key : map.keySet()) {
            Row row = map.get(key);
            log(command, row.getRow());
        }
    }

    public void closeConnection() throws IOException {
        printWriter.close();
    }

    private static String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time = System.currentTimeMillis();
        Date currentDate = new Date(time);
        return sdf.format(currentDate);
    }

}