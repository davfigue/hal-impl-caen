package org.fosstrack.hal.impl.caen.connector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.fosstrak.hal.impl.caen.CaenController;

/**
 * 
 * @author Pablo Piñeiro Rey
 * @author David Figueroa Escalante
 */
public class TCPRawSocketConnector {
	
	static Logger log = Logger.getLogger(TCPRawSocketConnector.class);
	
	/**
	 * 
	 */
	CaenController controller;
	
	/**
	 * TCP socket to the device
	 */
	Socket s;

	/**
	 * 
	 */
	SocketAddress address;

	/**
	 * Input stream to send data to the device
	 */
	BufferedReader in;

	/**
	 * Output stream to receive data from the device
	 */
	PrintWriter out;

	/**
	 * Shared synchronized resource between Producer and Consumer
	 */
	BlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>();

	boolean initialized = false;
	boolean operating = false;
	boolean autoReconnect;

	/**
	 * Producer of messages for the queue
	 */
	Producer producer;

	/**
	 * Consumer of messages from the queue
	 */
	Consumer consumer;
	

	/**
	 * 
	 * @param host
	 * @param port
	 * @param autoReconnect
	 */
	public TCPRawSocketConnector(String host, int port, boolean autoReconnect) {

		this.autoReconnect = autoReconnect;
		s = new Socket();
		address = new InetSocketAddress(host, port);
	}
	
	/**
	 *  Connector's socket string representation
	 */
	public String toString() {
		return s.getInetAddress().getHostAddress() + ":" + s.getPort();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.asi.middleware.rfid.RFIDDeviceConnector#establishConnection()
	 */
	public void establishConnection() throws IOException, Exception {
		
		if(controller == null) {
			throw new Exception("TCPConnector: No Controller bound to yet");
		}
		
		s.connect(address);
		log.info("TCPConnector: connection established");

		in = new BufferedReader(new InputStreamReader(s.getInputStream()));
		out = new PrintWriter(new OutputStreamWriter(s.getOutputStream()), true);

		producer = new Producer(messageQueue, this, s);
		consumer = new Consumer(messageQueue, this);

		consumer.start();
		producer.start();

		initialized = true;
	}
	
	/*
	 * 
	 * @throws IOException
	 */
	private void reconnect() throws IOException {
		
		s = new Socket();
		
		try {
			
			s.connect(address);
			
			in = new BufferedReader(new InputStreamReader(s.getInputStream()));
			out = new PrintWriter(new OutputStreamWriter(s.getOutputStream()), true);
			
			producer = new Producer(messageQueue, this, s);
			
			producer.start();
			
			initialized = true;
			
		} catch (IOException ioex) {
			
			throw new IOException("Connector I/O error:" + ioex.getMessage(), ioex);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.asi.middleware.rfid.RFIDDeviceConnector#sendData(java.lang.String)
	 */
	public void sendData(String data) throws IOException {

		if (!s.isConnected() || s.isOutputShutdown()) {
			throw new IOException("TCPConnector Error: Socket comm error");
		}

		log.info("TCPConnector:send-data: " + data);

		out.println(data);
		out.flush();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.asi.middleware.rfid.RFIDDeviceConnector#dataReceived(java.lang.String
	 * )
	 */
	public void dataReceived(String message) {

		log.info("TCPConnector: message received from " + this.toString() + ", " + message);

		//reader.getDriver().cbNewMessageReceived(this, message);
		
		// Callback al controller
	}

	/**
	 * 
	 */
	public void connectionLost() {

		operating = initialized = false;

		try {
			if (s != null && !s.isInputShutdown()) {
				in.close();
			}
			if (!s.isOutputShutdown()) {
				out.close();
			}
			if (!s.isClosed()) {
				s.close();
			}

		} catch (IOException ex) {
			
			log.warn(ex.getMessage());
		}

		//reader.getDriver().cbConnectionLost(this);
		// Controller callback connection lost

		if (autoReconnect) {
			loopUntilConnected();
		}
	}

	/**
	 * Tries to reconnect
	 */
	private void loopUntilConnected() {

		while (!initialized) {
			
			log.info("TCPConnector No fatal: trying to reconnect. ");
			
			try {
				
				Thread.sleep(1000);
				reconnect();

			} catch (Exception ex) { }
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.asi.middleware.rfid.RFIDDeviceConnector#close()
	 */
	public void close() throws IOException {

		try {
			in.close();
			out.close();

		} catch (IOException ioex) {
			log.error("TCPConnector no fatal: Error closing I/O streams. ");
		}

		if (!s.isClosed()) {
			s.close();
		}
	}
}

/**
 * Implementation of producer in producer-consumer pattern for the access of
 * reply messages send by the device
 */
class Producer extends Thread {

	/**
	 * TCP connector instance
	 */
	TCPRawSocketConnector connector;

	private boolean finished = false;

	/**
	 * Shared synchronized resource
	 */
	BlockingQueue<String> messageQueue;

	/**
	 * Connector's socket to device
	 */
	Socket s;

	/**
	 * Socket's input stream
	 */
	BufferedReader in;

	public Producer(BlockingQueue<String> queue, TCPRawSocketConnector connector, Socket s) {

		messageQueue = queue;
		this.connector = connector;
		this.s = s;

		try {
			in = new BufferedReader(new InputStreamReader(s.getInputStream()));

		} catch (Exception ex) {
			TCPRawSocketConnector.log.error("TCPConnector - Producer. Producer Input Stream creation error");
		}
	}

	public void run() {

		String message;

		while (!finished) {

			if (s.isClosed() || !s.isConnected() || s.isInputShutdown()) {
				
				this.finish(true);

			} else {

				try {
					message = in.readLine();
					messageQueue.put(message);

				} catch (NullPointerException ex) {
					
					TCPRawSocketConnector.log.error("TCPConnector - Producer. Socket read error, remote host closed the socket");
					
					this.finish(true);

				} catch (Exception ex) {
					TCPRawSocketConnector.log.error("TCPConnector - Producer. Socket read error:");
					
					this.finish(true);
				}
			}
		}
	}

	public void finish(boolean finished) {
		this.finished = finished;
		connector.connectionLost();
	}
}

/**
 * Implementation of Consumer in producer-consumer pattern for the access of
 * reply messages sends by the device, when a message arrives invokes
 * dataReceived on TcpConnector
 */
class Consumer extends Thread {

	/**
	 * Connector shared synchronized resource
	 */
	BlockingQueue<String> queue;

	/**
	 * Connector's socket to device
	 */
	TCPRawSocketConnector connector;

	boolean finished = false;

	/**
	 * 
	 * @param queue
	 * @param connector
	 */
	public Consumer(BlockingQueue<String> queue, TCPRawSocketConnector connector) {

		this.queue = queue;
		this.connector = connector;
	}

	public void run() {

		String message = "";

		while (!finished) {

			try {
				message = queue.take();
				connector.dataReceived(message);

			} catch (InterruptedException iex) {
				iex.printStackTrace();
			}
		}
	}
}
