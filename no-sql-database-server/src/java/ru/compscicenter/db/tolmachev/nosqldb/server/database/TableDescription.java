package ru.compscicenter.db.tolmachev.nosqldb.server.database;

import java.util.List;

public class TableDescription implements Comparable<TableDescription> {

    //name of the table
    private String name;

    // list with column names
    private List<String> columnNames;

    public TableDescription(String name, List<String> columnNames) {
        this.name = name;
        this.columnNames = columnNames;
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public int compareTo(TableDescription that) {
        return name.compareTo(that.getName());
    }
}
