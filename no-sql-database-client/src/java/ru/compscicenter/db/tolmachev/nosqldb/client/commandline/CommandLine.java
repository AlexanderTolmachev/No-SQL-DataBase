package ru.compscicenter.db.tolmachev.nosqldb.client.commandline;

import ru.compscicenter.db.tolmachev.nosqldb.client.exceptions.HttpClientException;
import ru.compscicenter.db.tolmachev.nosqldb.client.httpprotocol.HttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CommandLine {

    private BufferedReader br;

    private HttpClient client;

    public CommandLine(String connectionHost, int connectionPort) {
        br = new BufferedReader(new InputStreamReader(System.in));
        client = new HttpClient(connectionHost, connectionPort);
    }

    public boolean establishConnection() {
        try {
            client.connectToServer();
        } catch (HttpClientException e) {
            return false;
        }
        return true;
    }

    public void execute() {
        String commandWithArgs;
        System.out.println("Enter some command: ");
        while (true) {
            try {
                commandWithArgs = br.readLine();

                String result = executeCommand(commandWithArgs);
                System.out.println(result);
                if ("exit".equals(result)) {
                    break;
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String executeCommand(String commandAsStr) {
        String response;
        try {
            response = client.sendRequestToServer(commandAsStr);
        } catch (HttpClientException e) {
            response = e.getMessage();
        }
        return response;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Invalid parameters number");
            System.out.println("Usage:");
            System.out.println("ru.compscicenter.db.tolmachev.nosqldb.client.commandline.CommandLine connection_host connection_port");
            return;
        }

        String connectionHost = args[0];
        int connectionPort = Integer.parseInt(args[1]);

        CommandLine commandLine = new CommandLine(connectionHost, connectionPort);

        System.out.println("Connecting to database server...");
        System.out.println("Host: " + connectionHost);
        System.out.println("Port: " + connectionPort);

        if (!commandLine.establishConnection()) {
            System.out.println("Connection to database server failed");
            return;
        }
        System.out.println("Connection to database server established");

        commandLine.execute();
    }
}
