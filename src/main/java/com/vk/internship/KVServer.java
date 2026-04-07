package com.vk.internship;


import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KVServer
{

    public static final Logger logger = LoggerFactory.getLogger(KVServer.class);

    public static void main(String[] args) throws Exception
    {
        String host = System.getenv().getOrDefault("TARANTOOL_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("TARANTOOL_PORT", "3301"));
        String username = System.getenv().getOrDefault("TARANTOOL_USER", "guest");
        String password = System.getenv().getOrDefault("TARANTOOL_PASS", "");
        String spaceName = System.getenv().getOrDefault("TARANTOOL_SPACE", "KV");

        int grpcPort = Integer.parseInt(System.getenv().getOrDefault("GRPC_SERVER_PORT", "8080"));

        KVRepository repository = new KVRepository(host, port, username, password, spaceName);
        KVServiceImpl service = new KVServiceImpl(repository);

        Server server = ServerBuilder.forPort(grpcPort).addService(service).build();

        server.start();
        logger.info("gRPC server started on port 8080");

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            logger.info("Shutting down gRPC server...");
            service.close();
            server.shutdown();
        }));

        server.awaitTermination();
    }
}
