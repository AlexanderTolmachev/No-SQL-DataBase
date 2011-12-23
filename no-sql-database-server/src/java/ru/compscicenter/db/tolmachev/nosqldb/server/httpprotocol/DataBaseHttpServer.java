package ru.compscicenter.db.tolmachev.nosqldb.server.httpprotocol;

import java.io.*;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import ru.compscicenter.db.tolmachev.nosqldb.server.command.CommandExecutor;
import ru.compscicenter.db.tolmachev.nosqldb.server.dbmanager.DataBaseManager;
import ru.compscicenter.db.tolmachev.nosqldb.server.exceptions.DataBaseServerException;

public class DataBaseHttpServer {
    private int serverPort;

    private HttpServer httpServer;

    private DataBaseManager dbManager;


    public DataBaseHttpServer(int serverPort) {
        this.serverPort = serverPort;
    }

    private String executeCommand(String commandWithArgs) {
        return CommandExecutor.execute(dbManager, commandWithArgs);
    }

    /**
     * starts server
     *
     * @throws DataBaseServerException - if something wrong
     */
    public void start() throws DataBaseServerException {
        initDBManager();
        try {
            httpServer = HttpServer.create(new InetSocketAddress(serverPort), 10);
        } catch (IOException exception) {
            throw new DataBaseServerException("Unable to start database server");
        }
        httpServer.createContext("/", new HttpRequestHandler());
        httpServer.start();
    }

    /**
     * load data from backup
     *
     * @throws DataBaseServerException - if something wrong
     */
    private void initDBManager() throws DataBaseServerException {
        dbManager = new DataBaseManager();
        System.out.println("Start loading data from backup...");
        try {
            dbManager.loadDataFromBackup();
        } catch (IOException e) {
            throw new DataBaseServerException("Unable to load data from backup", e);
        }
        System.out.println("Data from backup loaded successfully");
    }

    /**
     * stop server
     *
     * @throws DataBaseServerException - if something wrong
     */
    public void stop() throws DataBaseServerException {
        httpServer.stop(0);
        try {
            System.out.println("Making data backup...");
            dbManager.makeBackup();
            dbManager.stop();
            System.out.println("Backup is made successfully");
        } catch (IOException e) {
            throw new DataBaseServerException("Unable to write data to backup", e);
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Invalid parameters number");
            System.out.println("Usage:");
            System.out.println("ru.compscicenter.db.tolmachev.nosqldb.server.httpprotocol.DataBaseHttpServer database_server_port");
            return;
        }

        int dataBaseHttpServerPort = Integer.parseInt(args[0]);
        DataBaseHttpServer dataBaseHttpServer = new DataBaseHttpServer(dataBaseHttpServerPort);

        System.out.println("Starting database server on port " + dataBaseHttpServerPort + "...");

        try {
            dataBaseHttpServer.start();
        } catch (DataBaseServerException e) {
            System.out.println(e.getMessage());
            return;
        }

        System.out.println("Database server started");
        System.out.println("Press 'enter' to stop");

        try {
            System.in.read();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        try {
            dataBaseHttpServer.stop();
        } catch (DataBaseServerException e) {
            System.out.println(e.getMessage());
        }
        System.out.println("Database server stopped");
    }


    private class HttpRequestHandler implements HttpHandler {
        private static final String RESPONSE_TAG = "(response)";
        private static final String CONNECTION_TEST_MESSAGE = "<check connection>";
        private static final String CONNECTION_SUCCESS_MESSAGE = "<connection checked>";

        public void handle(HttpExchange exchange) throws IOException {
            String requestMethod = exchange.getRequestMethod();
            if (!"POST".equals(requestMethod)) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, 0);
                exchange.close();
                return;
            }

            InputStream inputStream = exchange.getRequestBody();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String requestBody = "";
            String line;

            while ((line = reader.readLine()) != null) {
                requestBody += line;
            }
            requestBody = requestBody.trim();
            inputStream.close();

            String responseString;
            if (CONNECTION_TEST_MESSAGE.equals(requestBody)) {
                responseString = RESPONSE_TAG + " " + CONNECTION_SUCCESS_MESSAGE;
            } else {
                responseString = RESPONSE_TAG + " " + executeCommand(requestBody);
            }

            Headers responseHeaders = exchange.getResponseHeaders();
            responseHeaders.add("Content-Type", "text/html");
            responseHeaders.add("Content-Length", Integer.toString(responseString.length()));
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, responseString.length() * 2);

            OutputStream outputStream = exchange.getResponseBody();
            outputStream.write(responseString.getBytes());
            outputStream.close();
            exchange.close();
        }
    }
}
