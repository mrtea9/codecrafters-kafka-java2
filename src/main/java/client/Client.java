package client;

import lombok.Getter;
import protocol.ExchangeMapper;
import protocol.Response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

@Getter
public class Client implements Runnable {

    private final ExchangeMapper mapper;
    private final Socket socket;
    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;

    public Client(ExchangeMapper mapper, Socket socket) throws IOException {
        this.mapper = mapper;
        this.socket = socket;

        this.inputStream = new DataInputStream(socket.getInputStream());
        this.outputStream = new DataOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        try (socket) {
            while (!socket.isClosed()) {
                this.outputStream.writeInt(7);
            }
        } catch (Exception exception) {
            System.err.println("%s: %s".formatted(socket.getLocalSocketAddress(), exception.getMessage()));

            if (!(exception instanceof EOFException)) {
                exception.printStackTrace();
            }
        }
    }

//    private void exchange() {
//        try {
//            mapper.sendResponse(outputStream, new Response(null, null));
//
//        }
//    }

}
