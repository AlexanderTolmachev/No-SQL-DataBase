package ru.compscicenter.db.tolmachev.nosqldb.server.command;

public enum Commands {

    ADD("add", 2, Integer.MAX_VALUE),
    GET("get", 2, 2),
    REMOVE("remove", 2, 2),
    UPDATE("update", 2, Integer.MAX_VALUE),
    CREATE("create", 2, Integer.MAX_VALUE),
    DROP("drop", 1, 1),
    DESC("desc", 1, 1),
    HELP("help", 0, 0),
    SHOW_ALL_TABLES("show_tables", 0, 0),
    UNKNOWN("unknown command", 1, 1),
    EXIT("exit", 0, 0);

    private String name;

    private int lowBoundArity;

    private int highBoundArity;


    private Commands(String name, int lowBoundArity, int highBoundArity) {
        this.name = name;
        this.lowBoundArity = lowBoundArity;
        this.highBoundArity = highBoundArity;
    }

    public String getName() {
        return name;
    }

    public boolean isArityAvailiable(int a) {
       return a >= lowBoundArity && a <= highBoundArity;
    }
}
