package HttpServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * A simple socket server
 * 
 */
public class MultiThreadedServer {

	private ServerSocket serverSocket;
	private int port;
	HttpGetDataInterface gdataInterface ;
	public MultiThreadedServer(int port,HttpGetDataInterface gdataInterface) {
		this.port = port;
		this.gdataInterface= gdataInterface;
	}

	public void start() throws IOException, InterruptedException {

		serverSocket = new ServerSocket(port);
		System.out.println("Starting the socket server at port:" + port);

		Socket client = null;

		while (true) {
			//System.out.println("Waiting for clients...");
			client = serverSocket.accept();
			//System.out.println("The following client has connected:"
			//		+ client.getInetAddress().getCanonicalHostName());
			// A client has connected to this server. Send welcome message
			Thread thread = new Thread(new SocketClientHandler(client,gdataInterface));
			thread.start();
		}
	}

}
