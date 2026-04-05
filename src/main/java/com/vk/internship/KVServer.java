package com.vk.internship;


import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.logging.Logger;

public class KVServer
{

    public static final Logger logger = Logger.getLogger(KVServer.class.getName());

    public static void main(String[] args) throws Exception
    {
        String host = "localhost";
        int port = 3301;
        String username = "guest";
        String password = "";

        KVServiceImpl service = new KVServiceImpl(host, port, username, password);

        Server server = ServerBuilder.forPort(8080).addService(service).build();

        server.start();
        logger.info("gRPC server started on port 8080");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down gRPC server...");
            service.close();
            server.shutdown();
    }));

        server.awaitTermination();
    }
}
