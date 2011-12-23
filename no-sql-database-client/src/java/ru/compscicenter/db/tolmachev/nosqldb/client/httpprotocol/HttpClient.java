package ru.compscicenter.db.tolmachev.nosqldb.client.httpprotocol;

import ru.compscicenter.db.tolmachev.nosqldb.client.exceptions.HttpClientException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

public class HttpClient {
    private static final String CONNECTION_TEST_MESSAGE = "<check connection>";
    private static final String CONNECTION_SUCCESS_MESSAGE = "<connection checked>";
    private static final String RESPONSE_TAG = "(response)";
    private static final String GENERAL_REQUEST_HEADER = "POST / HTTP/1.1";
    private static final String CONTENT_TYPE_REQUEST_HEADER = "Content-Type: application/x-www-form-urlencoded";
    private static final String USER_AGENT_REQUEST_HEADER = "HttpClient/1.1";
    private static final String REQUEST_HEADERS = GENERAL_REQUEST_HEADER + "\r\n" +
            CONTENT_TYPE_REQUEST_HEADER + "\r\n" +
            USER_AGENT_REQUEST_HEADER + "\r\n";

    private String connectionHost;
    private int connectionPort;

    public HttpClient(String connectionHost, int connectionPort) {
        this.connectionHost = connectionHost;
        this.connectionPort = connectionPort;
    }

    public void connectToServer() throws HttpClientException {
        String connectionTestResponse;
        try {
            connectionTestResponse = sendRequestToServer(CONNECTION_TEST_MESSAGE);
        } catch (HttpClientException e) {
            throw new HttpClientException("Connection to server failed", e);
        }

        if (!CONNECTION_SUCCESS_MESSAGE.equals(connectionTestResponse)) {
            throw new HttpClientException("Connection to server failed");
        }
    }

    public String sendRequestToServer(String requestBody) throws HttpClientException {
        Socket socket;
        String contentLengthHeader = "Content-Length: " + requestBody.length();
        String request = REQUEST_HEADERS + contentLengthHeader + "\r\n\r\n" + requestBody;

        try {
            socket = new Socket(connectionHost, connectionPort);
            socket.getOutputStream().write(request.getBytes());
        } catch (IOException e) {
            throw new HttpClientException("Unable to connect to server", e);
        }

        String response = new String();
        try {
            InputStream inputStream = socket.getInputStream();
            BufferedReader responseReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = responseReader.readLine()) != null) {
                response += line + "\r\n";
            }
            responseReader.close();
            socket.close();
        } catch (IOException e) {
            throw new HttpClientException("Unable to receive a response from server", e);
        }

        int responseTagStartPosition = response.indexOf(RESPONSE_TAG);
        if (responseTagStartPosition == -1) {
            throw new HttpClientException("Invalid response");
        }

        int responseTagEndPosition = responseTagStartPosition + RESPONSE_TAG.length();
        String responseString = response.substring(responseTagEndPosition).trim();

        return responseString;
    }
}
