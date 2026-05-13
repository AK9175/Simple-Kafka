package com.simplekafka.broker.network;

import com.simplekafka.broker.server.KafkaApis;
import com.simplekafka.common.log.ClusterMetadata;
import com.simplekafka.common.log.ClusterMetadataLog;
import com.simplekafka.common.requests.RequestHeader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SocketServer {
    private final int port;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();

    public SocketServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        ClusterMetadata metadata = ClusterMetadataLog.load();
        System.err.println("Listening on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            threadPool.submit(() -> handleClient(clientSocket, metadata));
        }
    }

    private void handleClient(Socket clientSocket, ClusterMetadata metadata) {
        try {
            DataInputStream in = new DataInputStream(clientSocket.getInputStream());
            DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
            while (true) {
                RequestHeader header = RequestHeader.readFrom(in);
                KafkaApis.handle(header, in, out, metadata);
            }
        } catch (IOException e) {
            System.err.println("Client disconnected: " + e.getMessage());
        } finally {
            try { clientSocket.close(); } catch (IOException e) { /* ignore */ }
        }
    }
}
