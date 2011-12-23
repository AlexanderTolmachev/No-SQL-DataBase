package ru.compscicenter.db.tolmachev.nosqldb.server.dbmanager;

import ru.compscicenter.db.tolmachev.nosqldb.server.database.DataBase;
import ru.compscicenter.db.tolmachev.nosqldb.server.database.Row;
import ru.compscicenter.db.tolmachev.nosqldb.server.database.TableDescription;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DBUnavailabilityException;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DataBaseRequestException;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DataBaseTableException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Balancer {

    private Map<Pattern, DataBase> databases;

    private static Balancer balancer;

    public static Balancer getInstance() {
        if (balancer == null) {
            balancer = new Balancer();
        }
        return balancer;
    }

    private Balancer() {
        DataBase db1 = new DataBase();
        DataBase db2 = new DataBase();
        DataBase db3 = new DataBase();

        databases = new HashMap<Pattern, DataBase>();
        databases.put(Pattern.compile("[a-mA-Mа-нА-Н]"), db1);
        databases.put(Pattern.compile("[n-zN-Zо-яО-Я]"), db2);
        databases.put(Pattern.compile("[^a-zA-ZA-Яа-я]"), db3);
    }

    /**
     * get descriptions of all tables in the database
     *
     * @return set of descriptions, where each descriptions - is a list that contains name of the table at 0 index,
     *         and column names. Key name has 1 index in list.
     */
    public Map<String, TableDescription> getTablesInDataBaseDescription() {
        Map<String, TableDescription> result = new HashMap<String, TableDescription>();

        for (DataBase db : databases.values()) {
            for (TableDescription td : db.getTablesDescriptions()) {
                String tableName = td.getName();
                if (!result.containsKey(tableName)) {
                    result.put(tableName, td);
                }
            }
        }
        return result;
    }

    /**
     * create table with given name and list of column names. Key name has 0 index in the list
     *
     * @param tableName   - name of the table
     * @param columnNames - name of columns as list of strings
     * @return - if table created successfully. false - if there already exists table with such name
     * @throws DataBaseTableException - if something wrong
     */
    public boolean createTable(String tableName, List<String> columnNames) throws DataBaseTableException {
        for (DataBase db : databases.values()) {
            boolean result = db.createTable(tableName, columnNames);
            if (!result) {
                return false;
            }
        }
        return true;
    }

    /**
     * create table with given description
     *
     * @param tableDescription - given description
     * @return true - if database modified
     * @throws DataBaseTableException - if something wrong
     */
    public boolean createTable(TableDescription tableDescription) throws DataBaseTableException {
        String tableName = tableDescription.getName();
        List<String> columnNames = tableDescription.getColumnNames();
        return createTable(tableName, columnNames);
    }

    /**
     * drop table with such name
     *
     * @param tableName - table name
     * @return true - if deletion successful. false - if there is no table with such name
     */
    public boolean dropTable(String tableName) {
        for (DataBase db : databases.values()) {
            boolean result = db.dropTable(tableName);
            if (!result) {
                return false;
            }
        }
        return true;
    }

    /**
     * add row to the table with given name
     *
     * @param tableName - name of the table
     * @param args      - row as list of args, key has 0 index
     * @throws ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DBUnavailabilityException - database is not available
     * @throws DataBaseRequestException   - wrong request
     * @throws DataBaseTableException     - if something wrong with adding data to th table
     */
    public void add(String tableName, List<String> args) throws DBUnavailabilityException, DataBaseRequestException,
            DataBaseTableException {
        if (args.isEmpty()) {
            throw new DataBaseRequestException("zero args in request list");
        }

        String key = args.get(0);
        if (key == null) {
            throw new DataBaseRequestException("key musn't be null");
        }

        String firstCharAtName = key.substring(0, 1);
        DataBase db = getDataBaseByKey(firstCharAtName);
        db.add(tableName, args);
    }

    /**
     * get row from table with given name by key
     *
     * @param tableName - table name
     * @param key       -
     * @return row
     * @throws ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DBUnavailabilityException - database is not available
     * @throws DataBaseRequestException   - wrong request
     * @throws DataBaseTableException     - if something wrong with adding data to th table
     */
    public Row getByKey(String tableName, String key) throws DBUnavailabilityException, DataBaseRequestException, DataBaseTableException {
        if (key == null) {
            throw new DataBaseRequestException("key musn't be null");
        }

        String firstCharAtName = key.substring(0, 1);
        DataBase db = getDataBaseByKey(firstCharAtName);
        return db.getByKey(tableName, key);
    }

    /**
     * get all rows from the table with given name
     *
     * @param tableName table name
     * @return all rows
     * @throws DataBaseTableException - if something wrong with adding data to th table
     */
    public Map<String, Row> getAll(String tableName) throws DataBaseTableException {
        Map<String, Row> resultMap = new HashMap<String, Row>();
        for (DataBase db : databases.values()) {
            Map<String, Row> allInCurrentDB = db.getAll(tableName);

            for (String key : allInCurrentDB.keySet()) {
                Row row = allInCurrentDB.get(key);

                resultMap.put(key, row);
            }
        }
        return resultMap;
    }

    /**
     * get description for the table with given name
     *
     * @param tableName - name of the table
     * @return column names as string list
     * @throws DataBaseTableException - if something wrong
     */
    public List<String> getTableDescription(String tableName) throws DataBaseTableException {
        TableDescription td = null;
        for (DataBase db : databases.values()) {
            td = db.getTableDescription(tableName);
            if (td != null) {
                return td.getColumnNames();
            }
        }
        return null;
    }

    /**
     * remove from table with given name by key
     *
     * @param tableName - table name
     * @param key       -
     * @return true - if databse modified
     * @throws ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DBUnavailabilityException - database is not available
     * @throws DataBaseRequestException   - wrong request
     * @throws DataBaseTableException     - if something wrong with adding data to th table
     */
    public boolean removeByKey(String tableName, String key) throws DBUnavailabilityException, DataBaseRequestException, DataBaseTableException {
        if (key == null) {
            throw new DataBaseRequestException("key musn't be null");
        }

        String firstCharAtName = key.substring(0, 1);
        DataBase db = getDataBaseByKey(firstCharAtName);
        return db.removeByKey(tableName, key);
    }

    /**
     * update row in the table with given name
     *
     * @param tableName - table name
     * @param args      - row
     * @return true - if database modified
     * @throws ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DBUnavailabilityException -  database is not available
     * @throws DataBaseRequestException   - wrong request
     * @throws DataBaseTableException     - if something wrong with adding data to th table
     */
    public boolean updateByName(String tableName, List<String> args) throws DBUnavailabilityException, DataBaseRequestException, DataBaseTableException {
        if (args.isEmpty()) {
            throw new DataBaseRequestException("zero args in request list");
        }

        String key = args.get(0);
        if (key == null) {
            throw new DataBaseRequestException("key shouldn't be null");
        }

        String firstCharAtName = key.substring(0, 1);
        DataBase db = getDataBaseByKey(firstCharAtName);
        return db.updateByKey(tableName, args);
    }

    /**
     * get all available table names from database
     *
     * @return - set with table names
     */
    public Set<String> getAllAvailableTableNames() {
        Set<String> result = new HashSet<String>();

        for (DataBase db : databases.values()) {
            Set<String> namesOfAvailableTablesInCurrentDB = db.getTableNames();
            result.addAll(namesOfAvailableTablesInCurrentDB);
        }
        return result;
    }

    private DataBase getDataBaseByKey(String key) throws DBUnavailabilityException {
        for (Pattern regexp : databases.keySet()) {
            Matcher matcher = regexp.matcher(key);
            if (matcher.find()) {
                return databases.get(regexp);
            }
        }
        throw new DBUnavailabilityException("database is unavaliable");
    }

}
