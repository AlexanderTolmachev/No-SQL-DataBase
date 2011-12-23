package ru.compscicenter.db.tolmachev.nosqldb.server.command;

import java.util.Collections;
import java.util.List;

public class Command {

    public Commands command;

    public List<String> args;

    public Command(Commands name, List<String> args) {
        this.command = name;
        this.args = args;
    }

    public Command(Commands name, String arg) {
        this.command = name;
        this.args = Collections.singletonList(arg);
    }

    public Command(Commands name) {
        this.command = name;
        this.args = Collections.emptyList();
    }

    public Commands getName() {
        return command;
    }

    public List<String> getArgs() {
        return args;
    }

    @Override
    public String toString() {
        return "Command{" +
                "name='" + command + '\'' +
                ", args=" + args +
                '}';
    }
}
