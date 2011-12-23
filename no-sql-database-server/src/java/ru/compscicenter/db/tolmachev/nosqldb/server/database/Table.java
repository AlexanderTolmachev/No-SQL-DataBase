package ru.compscicenter.db.tolmachev.nosqldb.server.database;

import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DataBaseTableException;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.InvalidTableRowException;

import java.util.*;

public class Table {

    private String name;

    private List<String> columnNames;

    private Map<String, Row> rows;

    public Table(String tableName, List<String> columnNames) throws DataBaseTableException {
        this.name = tableName;

        if (columnNames.size() == 0) {
            throw new DataBaseTableException("table must contain more than 0 columns");
        }

        this.columnNames = new ArrayList<String>();
        for (String columnName : columnNames) {
            this.columnNames.add(columnName);
        }

        this.rows = new HashMap<String, Row>();
    }

    /**
     * get name of the table
     *
     * @return table name
     */
    public String getName() {
        return name;
    }

    /**
     * get list of column's names
     *
     * @return list of names
     */
    public List<String> getColumnNames() {
        return columnNames;
    }

    /**
     * get table description as table name and column names
     *
     * @return table description
     */
    public TableDescription getTableDescription() {
        return new TableDescription(name, columnNames);
    }

    /**
     * add row to the table
     *
     * @param args - arguments
     * @return true - if collection modified
     * @throws InvalidTableRowException - if amount of args doesn't match column's size
     */
    public boolean add(List<String> args) throws InvalidTableRowException {
        if (args.size() != columnNames.size()) {
            throw new InvalidTableRowException("args massive size doesn't map table column size");
        }

        String key = args.get(0);

        Row row = new Row(args);
        rows.put(key, row);
        return true;
    }

    /**
     * get row by key
     *
     * @param key - primary key
     * @return row that maps to this key
     */
    public Row get(String key) {
        return rows.get(key);
    }

    /**
     * get all rows from the table
     *
     * @return all data in the table as map of key - row
     */
    public Map<String, Row> getAll() {
        return rows;
    }

    /**
     * remove row by key
     *
     * @param key -
     * @return true if table modified
     */
    public boolean remove(String key) {
        return rows.remove(key) != null;
    }

    /**
     * update row in table
     *
     * @param args - args as list. Element with index 0 - key.
     * @return true - if table modified
     * @throws InvalidTableRowException - if amount of args doesn't match column's size
     */
    public boolean update(List<String> args) throws InvalidTableRowException {
        if (args.size() != columnNames.size()) {
            throw new InvalidTableRowException("args massive size doesn't map table column size");
        }

        String key = args.get(0);
        Row row = new Row(args);

        rows.put(key, row);
        return true;
    }
}
