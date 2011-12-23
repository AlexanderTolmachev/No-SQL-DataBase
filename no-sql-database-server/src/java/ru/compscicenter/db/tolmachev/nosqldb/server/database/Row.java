package ru.compscicenter.db.tolmachev.nosqldb.server.database;

import java.util.List;

/**
 * row - it is a sequence of string, that belongs for one record in data base. row contains key!
 */
public class Row {

    List<String> row;

    public Row(List<String> row) {
        this.row = row;
    }

    public List<String> getRow() {
        return row;
    }

    public void setRow(List<String> row) {
        this.row = row;
    }
}
