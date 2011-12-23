package ru.compscicenter.db.tolmachev.nosqldb.server.database;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * container for all tables in database
 */
public class DataContainer {

    private Map<TableDescription, Map<String, Row>> data;

    public DataContainer() {
        this.data = new HashMap<TableDescription, Map<String, Row>>();
    }

    public void put(TableDescription tableDescription, Map<String, Row> rows) {
        data.put(tableDescription, rows);
    }

    public Set<TableDescription> getTableDescriptions() {
        return data.keySet();
    }

    public Map<String, Row> getRowsByDescription(TableDescription tableDescription) {
        return data.get(tableDescription);
    }
}
