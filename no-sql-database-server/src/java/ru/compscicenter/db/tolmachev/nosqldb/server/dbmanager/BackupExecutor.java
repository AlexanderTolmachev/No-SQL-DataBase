package ru.compscicenter.db.tolmachev.nosqldb.server.dbmanager;

import ru.compscicenter.db.tolmachev.nosqldb.server.database.DataContainer;
import ru.compscicenter.db.tolmachev.nosqldb.server.database.Row;
import ru.compscicenter.db.tolmachev.nosqldb.server.database.TableDescription;

import java.io.*;
import java.util.*;


public class BackupExecutor {

    private static final String backupFilePath = "backup" + File.separator + "backup.txt";

    /**
     * read all data for tables in database from backup file
     *
     * @return - container with all data
     * @throws IOException - if something wrong with reading data from file
     */
    public static DataContainer readTableDescriptions() throws IOException {
        DataContainer allData = new DataContainer();

        File fileDir = new File(backupFilePath);
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"));


        Map<String, Row> rowsForCurrentTable = null;
        TableDescription currentTableDescription = null;
        String str;
        while ((str = in.readLine()) != null) {
            if ("".equals(str)) {
                continue;
            }

            if (str.startsWith("table: ")) {
                String tableName = str.substring(7);
                String descriptionAsStr = in.readLine();
                List<String> descriptions = getColumnNames(descriptionAsStr);
                currentTableDescription = new TableDescription(tableName, descriptions);
                rowsForCurrentTable = new HashMap<String, Row>();
            } else if ("table end".equals(str)) {
                allData.put(currentTableDescription, rowsForCurrentTable);
                rowsForCurrentTable = new HashMap<String, Row>();
                currentTableDescription = null;
            } else {
                String[] rowAsStrMassive = str.split(";");
                String key = rowAsStrMassive[0];
                List<String> row = Arrays.asList(rowAsStrMassive);
                rowsForCurrentTable.put(key, new Row(row));
            }
        }


        in.close();
        return allData;
    }

    private static List<String> getColumnNames(String str) {
        String[] desc = str.split(";");
        List<String> descriptions = Arrays.asList(desc);
        return descriptions;
    }

    /**
     * write all database data to the file
     * @param dataContainer - container with all data in database
     * @throws IOException - if something wrong with reading data from file
     */
    public static void writeDataToFile(DataContainer dataContainer) throws IOException {
        File fileDir = new File(backupFilePath);
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileDir), "UTF-8"));

        Set<TableDescription> tableDescriptions = dataContainer.getTableDescriptions();
        Iterator<TableDescription> iterator = tableDescriptions.iterator();
        while (iterator.hasNext()) {
            TableDescription tableDescription = iterator.next();

            String tableName = tableDescription.getName();
            writeTableDescription(out, tableDescription);

            Map<String, Row> dataInTable = dataContainer.getRowsByDescription(tableDescription);
            writeTableRows(out, dataInTable);
            out.write("table end\n");
        }
        out.close();
    }

    private static void writeTableDescription(BufferedWriter out, TableDescription td) throws IOException {
        String tableName = td.getName();
        List<String> columnNames = td.getColumnNames();

        out.write("table: " + tableName + "\n");
        for (String columnName : columnNames) {
            out.write(columnName + ";");
        }
        out.write("\n");
    }

    private static void writeTableRows(BufferedWriter out, Map<String, Row> dataInTable) throws IOException {
        Set<String> keySet = dataInTable.keySet();
        for (String key : keySet) {
            Row row = dataInTable.get(key);
            List<String> r = row.getRow();

            for (String elem : r) {
                out.write(elem + ";");
            }
            out.write("\n");
        }
    }
}
