package ru.compscicenter.db.tolmachev.nosqldb.server.command;

import ru.compscicenter.db.tolmachev.nosqldb.server.database.Row;
import ru.compscicenter.db.tolmachev.nosqldb.server.dbmanager.DataBaseManager;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DBUnavailabilityException;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DataBaseRequestException;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DataBaseTableException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CommandExecutor {

    public static String execute(DataBaseManager dbManager, String commandWithArgs) {
        Command command = CommandParser.parse(commandWithArgs);
        Commands commandName = command.getName();

        String result = "";
        if (Commands.HELP.getName().equals(commandName.getName())) {
            result = executeHelpCommand();
        } else if (Commands.CREATE.getName().equals(commandName.getName())) {
            result = executeCreateTableCommand(dbManager, command);
        } else if (Commands.DROP.getName().equals(commandName.getName())) {
            result = executeDropTableCommand(dbManager, command);
        } else if (Commands.SHOW_ALL_TABLES.getName().equals(commandName.getName())) {
            result = executeShowAllTablesCommand(dbManager, command);
        } else if (Commands.DESC.getName().equals(commandName.getName())) {
            result = executeDescTableCommand(dbManager, command);
        } else if (Commands.ADD.getName().equals(commandName.getName())) {
            result = executeAddCommand(dbManager, command);
        } else if (Commands.GET.getName().equals(commandName.getName())) {
            result = executeGetCommand(dbManager, command);
        } else if (Commands.REMOVE.getName().equals(commandName.getName())) {
            result = executeRemoveCommand(dbManager, command);
        } else if (Commands.UPDATE.getName().equals(commandName.getName())) {
            result = executeUpdateCommand(dbManager, command);
        } else if (Commands.EXIT.getName().equals(commandName.getName())) {
            result = commandName.getName();
        } else if (Commands.UNKNOWN.getName().equals(commandName.getName())) {
            List<String> args = command.getArgs();
            result = args.get(0);
        } else {
            result = "unknown command";
        }
        return result;
    }

    private static String executeHelpCommand() {
        StringBuffer sb = new StringBuffer();
        sb.append("help - print all available commands\n");
        sb.append("show_tables - show names of all tables\n");
        sb.append("create (tableName:key, columnName1, ...) | create table:key, columnName1... - create table with given name and columns  \n");
        sb.append("drop (tableName) | drop tableName \n");
        sb.append("get (tableName:key) | get table:key - get list of rows that match given key\n");
        sb.append("update (tableName:key, value1 ...) | update table:key, value1 -  update row in table with given key\n");
        sb.append("add (tableName:key, value1 ...) | add tableName:key, value1 -  add row to table with given key and values\n");
        sb.append("remove (tableName:key) | remove tableName:key - remove from table row with given key by name\n");
        sb.append("desc (tableName) | desc tableName - describe table with given name\n");
        sb.append("exit - safe exit with making backups");
        return sb.toString();
    }

    private static String executeShowAllTablesCommand(DataBaseManager dbManager, Command command) {
        Collection<String> tableNamesCollection = dbManager.getAllTableNames();

        String result = "";
        for (String s : tableNamesCollection) {
            result += s;
            result += "\n";
        }
        return result;
    }

    private static String executeCreateTableCommand(DataBaseManager dbManager, Command command) {
        List<String> args = command.getArgs();
        String tableName = args.get(0);
        List<String> columnNames = args.subList(1, args.size());

        String msg = "";
        try {
            boolean result = dbManager.createTable(tableName, columnNames);
            if (result) {
                msg = "table " + tableName + " created successfully";
            } else {
                msg = "table " + tableName + " couldn't be created";
            }
        } catch (DataBaseTableException e) {
            msg = e.getMessage();
        }
        return msg;
    }

    private static String executeDropTableCommand(DataBaseManager dbManager, Command command) {
        List<String> args = command.getArgs();
        String tableName = args.get(0);

        String msg = "";
        boolean result = dbManager.dropTable(tableName);
        if (result) {
            msg = "table " + tableName + " dropped successfully";
        } else {
            msg = "table " + tableName + " couldn't be dropped";
        }
        return msg;
    }

    private static String executeDescTableCommand(DataBaseManager dbManager, Command command) {
        List<String> args = command.getArgs();
        String tableName = args.get(0);

        String msg = "";
        try {
            List<String> result = dbManager.describeTable(tableName);
            msg = getResultSetAsStr(result);
        } catch (DataBaseTableException e) {
            msg = e.getMessage();
        }
        return msg;
    }

    private static String executeAddCommand(DataBaseManager dbManager, Command command) {
        List<String> args = command.getArgs();

        String tableName = args.get(0);
        List<String> params = args.subList(1, args.size());

        String msg;
        try {
            dbManager.save(tableName, params);
            msg = "data saved";
        } catch (DBUnavailabilityException e) {
            msg = e.getMessage();
        } catch (DataBaseTableException e) {
            msg = e.getMessage();
        } catch (DataBaseRequestException e) {
            msg = e.getMessage();
        }
        return msg;
    }


    private static String executeGetCommand(DataBaseManager dbManager, Command command) {
        List<String> args = command.getArgs();
        String tableName = args.get(0);
        String key = args.get(1);

        String msg = "";
        try {
            if ("*".equals(key)) {
                Map<String, Row> result = dbManager.getAll(tableName);
                msg = getResultSetAsStr(result);
            } else {
                List<String> result = dbManager.get(tableName, key);
                msg = getResultSetAsStr(result);
            }
        } catch (DataBaseTableException e) {
            msg = e.getMessage();
        } catch (DBUnavailabilityException e) {
            msg = e.getMessage();
        } catch (DataBaseRequestException e) {
            msg = e.getMessage();
        }
        return msg;
    }


    private static String executeRemoveCommand(DataBaseManager dbManager, Command command) {
        List<String> args = command.getArgs();
        String tableName = args.get(0);
        String key = args.get(1);

        String msg = "";

        try {
            boolean modified = dbManager.removeByKey(tableName, key);
            if (modified) {
                msg = "row with key " + tableName + ":" + key + " removed";
            } else {
                msg = "no rows with key " + tableName + ":" + key;
            }
        } catch (DBUnavailabilityException e) {
            msg = e.getMessage();
        } catch (DataBaseRequestException e) {
            msg = e.getMessage();
        } catch (DataBaseTableException e) {
            msg = e.getMessage();
        }
        return msg;
    }


    private static String executeUpdateCommand(DataBaseManager dbManager, Command command) {
        List<String> args = command.getArgs();

        String tableName = args.get(0);
        List<String> params = args.subList(1, args.size());

        String msg;
        try {
            dbManager.updateByKey(tableName, params);
            msg = "data updated";
        } catch (DBUnavailabilityException e) {
            msg = e.getMessage();
        } catch (DataBaseTableException e) {
            msg = e.getMessage();
        } catch (DataBaseRequestException e) {
            msg = e.getMessage();
        }
        return msg;
    }


    private static String getResultSetAsStr(Map<String, Row> rowsMap) {
        StringBuffer result = new StringBuffer();
        if (rowsMap.isEmpty()) {
            result.append("set is empty");
        } else {
            Set<String> keySet = rowsMap.keySet();
            for (String key : keySet) {
                Row row = rowsMap.get(key);
                List<String> list = row.getRow();
                for (String elem : list) {
                    result.append(elem);
                    result.append(" ");
                }
                result.append("\n");
            }
        }
        return result.toString();
    }

    private static String getResultSetAsStr(Collection<String> row) {
        StringBuffer result = new StringBuffer();
        if (row.isEmpty()) {
            result.append("set is empty");
        } else {
            for (String elem : row) {
                result.append(elem);
                result.append(" ");
            }
            result.append("\n");
        }
        return result.toString();
    }
}