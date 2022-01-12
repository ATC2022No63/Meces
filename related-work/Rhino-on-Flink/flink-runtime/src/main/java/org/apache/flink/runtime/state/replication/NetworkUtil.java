package org.apache.flink.runtime.state.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;


public class NetworkUtil {
    protected static final Logger log = LoggerFactory.getLogger(NetworkUtil.class);

    private static final int defaultPort = 23456;

    public static void post(int port, byte[] bytes) {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            Socket conn = serverSocket.accept();
            OutputStream outputStream = conn.getOutputStream();
            byte[] len = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder()).putInt(bytes.length).array();
            log.info("post {} bytes,{}", bytes.length, Arrays.toString(len));

            outputStream.write(len);
            outputStream.write(bytes);
            conn.close();
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void post(byte[] bytes) {
        post(defaultPort, bytes);
    }

    public static byte[] fetch(InetAddress server) throws IOException {
        return fetch(defaultPort, server);
    }

    public static byte[] fetch(int port, InetAddress server) throws IOException {
        int retries = 100;
        Socket socket = null;

        while (true) {
            try {
                socket = new Socket(server, port);
                break;
            } catch (IOException e) {
                retries--;
                if (retries == 0)
                    throw new IOException("too many fails");
            }

        }
        InputStream inputStream = socket.getInputStream();
        byte[] lenBuffer = new byte[4];
        readn(inputStream, lenBuffer, 4);
        int len = ByteBuffer.wrap(lenBuffer).order(ByteOrder.nativeOrder()).getInt();
        log.info("should fetch {},{} \n", len, Arrays.toString(lenBuffer));

        byte[] buffer = new byte[len];

        readn(inputStream, buffer, len);
        socket.close();
        return buffer;
    }

    public static void readn(InputStream inputStream, byte[] buffer, int n) throws IOException {
        int received = 0;
        while (n > received) {
            int read = inputStream.read(buffer, received, n - received);
            received += read;
        }
        log.info("readn done");
    }
}

