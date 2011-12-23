package ru.compscicenter.db.tolmachev.nosqldb.server.command;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CommandParser {


    public static Command parse(String commandAsStr) {
        commandAsStr = commandAsStr.trim();
        if ("".equals(commandAsStr)) {
            return new Command(Commands.UNKNOWN, "unknown command");
        }

        String commandName = getCommandName(commandAsStr);
        List<String> args = getArgs(commandAsStr);

        int commandArity = args.size();

        for (Commands command : Commands.values()) {
            if (command.getName().equals(commandName) && command.isArityAvailiable(commandArity)) {
                return new Command(command, args);
            } else if (command.getName().equals(commandName) && !command.isArityAvailiable(commandArity)) {
                return new Command(Commands.UNKNOWN, "command " + commandName + " mustn't have " + commandArity + " arguments");
            }
        }
        return new Command(Commands.UNKNOWN, "unknown command name. Enter help command to see avaliable commands");
    }

    private static String getCommandName(String commandAsStr) {
        int whiteSpaceIndex = commandAsStr.indexOf(" ");
        if (whiteSpaceIndex == -1) {
            return commandAsStr;
        }

        String commandName = commandAsStr.substring(0, whiteSpaceIndex);
        return commandName.trim();
    }

    private static List<String> getArgs(String commandAsStr) {
        int whiteSpaceIndex = commandAsStr.indexOf(" ");
        if (whiteSpaceIndex == -1) {
            return Collections.emptyList();
        }

        String argsStr = commandAsStr.substring(whiteSpaceIndex);
        argsStr = argsStr.trim();

        if ("".equals(argsStr)) {
            return Collections.emptyList();
        }

        if (argsStr.startsWith("(") && argsStr.endsWith(")")) {
            argsStr = argsStr.substring(1, argsStr.length() - 1);
        }

        String[] argsMass = argsStr.split("[,:]");
        for(String arg : argsMass){
            arg = arg.trim();
        }

        return Arrays.asList(argsMass);
    }
}
