package ru.compscicenter.db.tolmachev.nosqldb.server.exceptions;

public class InvalidTableRowException extends DataBaseTableException {

    public InvalidTableRowException() {
    }

    public InvalidTableRowException(String message) {
        super(message);
    }

    public InvalidTableRowException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidTableRowException(Throwable cause) {
        super(cause);
    }
}
