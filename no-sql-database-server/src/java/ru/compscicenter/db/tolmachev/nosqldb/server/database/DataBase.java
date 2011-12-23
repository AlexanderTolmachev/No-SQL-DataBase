package ru.compscicenter.db.tolmachev.nosqldb.server.database;


import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DataBaseTableException;

import java.util.*;

public class DataBase {

    private HashMap<String, Table> tableHashMap;


    public DataBase() {
        this.tableHashMap = new HashMap<String, Table>();
    }

    /**
     * create a table in database
     *
     * @param tableName   - name of the table
     * @param columnNames - list of the column names in table
     * @return true - if table created successfully
     * @throws DataBaseTableException - if table couldn't be created.
     *                                For example creation a table with zero columns restricted
     */
    public boolean createTable(String tableName, List<String> columnNames) throws DataBaseTableException {
        if (tableHashMap.containsKey(tableName)) {
            return false;
        }

        Table table = new Table(tableName, columnNames);
        tableHashMap.put(tableName, table);
        return true;
    }

    /**
     * get all tables in database descriptions as list with table name and column names
     *
     * @return list of descriptions
     */
    public Set<TableDescription> getTablesDescriptions() {
        Set<String> tableNameSet = tableHashMap.keySet();
        Set<TableDescription> tableDescriptions = new TreeSet<TableDescription>();
        for (String tableName : tableNameSet) {
            Table table = tableHashMap.get(tableName);
            TableDescription tableDescription = table.getTableDescription();
            tableDescriptions.add(tableDescription);
        }
        return tableDescriptions;
    }

    /**
     * get description of the table by name
     * @param tableName - name of the table
     * @return description of the table
     * @throws DataBaseTableException - if table with such name doesn't exist
     */
    public TableDescription getTableDescription(String tableName)  throws DataBaseTableException {
        Table table = tableHashMap.get(tableName);

        if (table == null) {
            throw new DataBaseTableException("no such table");
        }
        return table.getTableDescription();
    }

    /**
     * drop table with name
     *
     * @param tableName - name of the table
     * @return true - if modification in database successful,
     *         false - no table with this name
     */
    public boolean dropTable(String tableName) {
        if (!tableHashMap.containsKey(tableName)) {
            return false;
        }

        tableHashMap.remove(tableName);
        return true;
    }

    /**
     * get row from table that match key
     *
     * @param tableName - name of the table
     * @param key       - key
     * @return - row from table that matches key
     * @throws DataBaseTableException - if table with such name doesn't exist
     */
    public Row getByKey(String tableName, String key) throws DataBaseTableException {
        Table table = tableHashMap.get(tableName);
        if (table == null) {
            throw new DataBaseTableException("no such table");
        }

        return table.get(key);
    }

    /**
     * add row to the table
     *
     * @param tableName - name of the table
     * @param args      - row as list of strings, 0 index int the table is key
     * @return true - if table modified
     * @throws DataBaseTableException - if table with such name doesn't exist
     */
    public boolean add(String tableName, List<String> args) throws DataBaseTableException {
        Table table = tableHashMap.get(tableName);
        if (table == null) {
            throw new DataBaseTableException("no such table");
        }

        return table.add(args);
    }

    /**
     * get all rows from the table with given name
     *
     * @param tableName - name of the table
     * @return - all rows
     * @throws DataBaseTableException - if table with such name doesn't exist
     */
    public Map<String, Row> getAll(String tableName) throws DataBaseTableException {
        Table table = tableHashMap.get(tableName);
        if (table == null) {
            throw new DataBaseTableException("no such table");
        }

        return table.getAll();
    }

    /**
     * remove row that matches key from the table with given name
     *
     * @param tableName - name of the table
     * @param key       - name of the key
     * @return true - if table modified
     * @throws DataBaseTableException - if table with such name doesn't exist
     */
    public boolean removeByKey(String tableName, String key) throws DataBaseTableException {
        Table table = tableHashMap.get(tableName);
        if (table == null) {
            throw new DataBaseTableException("no such table");
        }
        return table.remove(key);
    }

    /**
     * update row int the table with given name
     *
     * @param tableName - name of the table
     * @param args      - row as list of strings, at 0 index is key
     * @return true - if table modified
     * @throws DataBaseTableException -  if table with such name doesn't exist
     */
    public boolean updateByKey(String tableName, List<String> args) throws DataBaseTableException {
        Table table = tableHashMap.get(tableName);
        if (table == null) {
            throw new DataBaseTableException("no such table");
        }
        return table.update(args);
    }

    public Set<String> getTableNames() {
        return tableHashMap.keySet();
    }
}
