package com.simplekafka.broker.server;

import com.simplekafka.broker.network.SocketServer;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        System.err.println("Logs from your program will appear here!");
        new SocketServer(9092).start();
    }
}
