package ru.compscicenter.db.tolmachev.nosqldb.server.dbmanager;

import ru.compscicenter.db.tolmachev.nosqldb.server.database.DataContainer;
import ru.compscicenter.db.tolmachev.nosqldb.server.database.Row;
import ru.compscicenter.db.tolmachev.nosqldb.server.database.TableDescription;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DBUnavailabilityException;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DataBaseRequestException;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DataBaseServerException;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DataBaseTableException;

import java.io.*;
import java.util.*;

public class DataBaseManager {

    private static final String systemLogPath = "logs" + File.separator + "systemLog.txt";

    private static final String undoLogPath = "logs" + File.separator + "undoLog.txt";

    private static Balancer balancer;

    private CommandLogger systemLogger;

    private CommandLogger undoLogger;

    private List<CommandLogger> loggers = new LinkedList<CommandLogger>();

    static {
        balancer = Balancer.getInstance();
    }

    public DataBaseManager() throws DataBaseServerException {
        undoLogger = initLogger("undoLog", undoLogPath);
        systemLogger = initLogger("systemLog", systemLogPath);
        loggers.add(undoLogger);
        loggers.add(systemLogger);
    }

    public void makeBackup() throws IOException {
        log(loggers, "starting making backup: ");

        DataContainer allData = getFromAllTablesInDB();
        BackupExecutor.writeDataToFile(allData);
        log(loggers, "backup made successfully:");
    }

    public void loadDataFromBackup() throws IOException {

        log(loggers, "starting loading data from backup: ");
        DataContainer allData = BackupExecutor.readTableDescriptions();

        for (TableDescription tableDescription : allData.getTableDescriptions()) {
            try {
                createTable(tableDescription);
                addDataToTable(tableDescription, allData);
            } catch (DataBaseTableException e) {
                log(loggers, "unable to create table with name: " + tableDescription.getName());
            }
        }
        log(loggers, "data from backup loaded successfully ");
    }

    public boolean createTable(String tableName, List<String> columnNames) throws DataBaseTableException {
        log(loggers, "create table " + tableName + " with columns: " + collectionsAsString(columnNames));
        TableDescription td = new TableDescription(tableName, columnNames);
        boolean result = createTable(td);
        log(loggers, "table created: " + result);
        return result;
    }

    private boolean createTable(TableDescription tableDescription) throws DataBaseTableException {
        return balancer.createTable(tableDescription);
    }

    public boolean dropTable(String tableName) {
        systemLogger.log("drop table " + tableName);
        boolean result = balancer.dropTable(tableName);
        log(loggers, "table dropped: " + result);
        return result;
    }

    public List<String> describeTable(String tableName) throws DataBaseTableException {
        systemLogger.log("describe table " + tableName);
        return balancer.getTableDescription(tableName);
    }

    public Collection<String> getAllTableNames() {
        Map<String, TableDescription> allTableDescriptions = balancer.getTablesInDataBaseDescription();
        return allTableDescriptions.keySet();
    }

    private void addDataToTable(TableDescription tableDescription, DataContainer allData) {
        Map<String, Row> map = allData.getRowsByDescription(tableDescription);
        String tableName = tableDescription.getName();

        for (String key : map.keySet()) {
            Row row = map.get(key);
            try {
                balancer.add(tableName, row.getRow());
            } catch (DBUnavailabilityException e) {
                log(loggers, "problem while loading data to database: database unavailable", row.getRow());
            } catch (DataBaseRequestException e) {
                log(loggers, "problem while loading data to database: wrong request", row.getRow());
            } catch (DataBaseTableException e) {
                log(loggers, "problem while loading data to database: table corruption", row.getRow());
            }
        }
    }


    public void save(String tableName, List<String> list) throws DBUnavailabilityException, DataBaseRequestException, DataBaseTableException {
        log(loggers, "save to table" + tableName, list);
        balancer.add(tableName, list);
    }

    public List<String> get(String tableName, String key) throws DBUnavailabilityException, DataBaseRequestException, DataBaseTableException {
        systemLogger.log("get from table " + tableName + " by key: " + key);
        Row row = balancer.getByKey(tableName, key);
        systemLogger.log("result set by key: " + key, row.getRow());
        return row.getRow();
    }

    public Map<String, Row> getAll(String tableName) throws DataBaseTableException {
        systemLogger.log("get all data");
        Map<String, Row> allRowsFromTable = balancer.getAll(tableName);
        systemLogger.log("result set: ", allRowsFromTable);
        return allRowsFromTable;
    }

    public boolean removeByKey(String tableName, String key) throws DBUnavailabilityException, DataBaseRequestException, DataBaseTableException {
        systemLogger.log("remove from table " + tableName + " by key: " + key);
        boolean isModified = balancer.removeByKey(tableName, key);
        log(loggers, "collection modified: " + isModified);
        return isModified;
    }

    public boolean updateByKey(String tableName, List<String> list) throws DBUnavailabilityException, DataBaseRequestException, DataBaseTableException {
        systemLogger.log("update in table " + tableName, list);
        boolean isModified = balancer.updateByName(tableName, list);
        log(loggers, "collection modified: " + isModified);
        return isModified;
    }

    public void stop() throws IOException {
        systemLogger.closeConnection();
        undoLogger.closeConnection();
    }

    private CommandLogger initLogger(String loggerName, String loggerFilePath) throws DataBaseServerException {
        try {
            return new CommandLogger(loggerName, loggerFilePath);
        } catch (IOException e) {
            throw new DataBaseServerException("Unable to initialize logger " + loggerName, e);
        }
    }

    private DataContainer getFromAllTablesInDB() {
        DataContainer allData = new DataContainer();
        Set<String> tableNames = balancer.getAllAvailableTableNames();
        Map<String, TableDescription> tableDescriptions = balancer.getTablesInDataBaseDescription();

        for (String tableName : tableNames) {
            try {
                Map<String, Row> allRowsInCurrentTable = balancer.getAll(tableName);
                TableDescription tableDescription = tableDescriptions.get(tableName);
                allData.put(tableDescription, allRowsInCurrentTable);
            } catch (DataBaseTableException e) {
                log(loggers, "unable to get data from table: " + tableName);
            }
        }
        return allData;
    }

    private void log(Collection<CommandLogger> loggers, String stringToLog) {
        for (CommandLogger logger : loggers) {
            logger.log(stringToLog);
        }
    }

    private void log(Collection<CommandLogger> loggers, String stringToLog, Collection<String> list) {
        for (CommandLogger logger : loggers) {
            logger.log(stringToLog, list);
        }
    }

    private String collectionsAsString(List<String> list) {
        String str = "( ";
        Iterator<String> iterator = list.iterator();
        while (iterator.hasNext()) {
            String s = iterator.next();
            str += s;
            if (iterator.hasNext()) {
                str += " ,";
            }
        }
        str += " )";
        return str;
    }
}
