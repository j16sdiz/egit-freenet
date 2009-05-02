/*
 * Copyright (C) 2008, CHENG Yuk-Pong, Daniel <j16sdiz+freenet@gmail.com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 *
 * - Neither the name of the Git Development Community nor the
 *   names of its contributors may be used to endorse or promote
 *   products derived from this software without specific prior
 *   written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.spearce.jgit.transport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.spearce.jgit.lib.ProgressMonitor;
import org.spearce.jgit.util.TemporaryBuffer;

/**
 * Freenet Client Protocol (FCP) 2.0 Client
 * <p>
 * See <a href="http://wiki.freenetproject.org/FreenetFCPSpec2Point0">Freenet
 * Client Protocol 2.0 Specification</a> for detail on this protocol.
 */
public class FreenetFCP {
	/** Default FCP port */
	public static final int DEFAULT_FCP_PORT = 9481;

	private InetAddress addr;

	private int port;

	private Socket socket;

	private InputStream is;

	private OutputStream os;

	/**
	 * Create a new FCP Connection to default host and port (
	 * <code>localhost:9481</code>)
	 * 
	 * @throws UnknownHostException
	 *             if no IP address for the <code>localhost</code> could be
	 *             found.
	 */
	public FreenetFCP() throws UnknownHostException {
		this(InetAddress.getAllByName("127.0.0.1")[0], DEFAULT_FCP_PORT);
	}

	/**
	 * Create a new FCP Connection to the specified IP address and port
	 * 
	 * @param address
	 *            the node IP address
	 * @param port
	 *            the port number
	 */
	public FreenetFCP(InetAddress address, int port) {
		this.addr = address;
		this.port = port;
	}

	/**
	 * Connect to the node
	 * 
	 * @throws IOException
	 *             if any I/O error occurred
	 */
	public void connect() throws IOException {
		socket = new Socket(addr, port);

		is = new BufferedInputStream(socket.getInputStream());
		os = new BufferedOutputStream(socket.getOutputStream());
	}

	/**
	 * Hello Message
	 * 
	 * Send handshake <code>ClientHello</code> message to node. Block until
	 * <code>NodeHello</code> is received.
	 * 
	 * @param clientName
	 *            Client name, must be unique in the freenet node
	 * @throws IOException
	 *             if any I/O error occurred
	 */
	public void hello(String clientName) throws IOException {
		Message msg = new Message();
		msg.type = "ClientHello";
		msg.field.put("ExpectedVersion", "2.0");
		msg.field.put("Name", clientName);

		send(msg);

		while (true) {
			Message reply = read(true);
			if ("NodeHello".equals(reply.type))
				return;
			if ("ProtocolError".equals(reply.type))
				throw new IOException("FCP error");
		}
	}

	Message simplePut(String freenetURI, TemporaryBuffer data,
			ProgressMonitor monitor, String monitorTask) throws IOException {
		Message msg = new Message();
		msg.type = "ClientPut";
		msg.field.put("URI", freenetURI);
		msg.field.put("Identifier", freenetURI);
		msg.field.put("Verbosity", monitor == null ? "0" : "1");
		msg.field.put("PriorityClass", "1");
		msg.field.put("Global", "false");
		msg.field.put("EarlyEncode", "true"); // for progress
		msg.field.put("UploadFrom", "direct");
		msg.field.put("DataLength", "" + data.length());
		msg.extraData = data;

		send(msg);

		int totalBlocks = -1;
		int completedBlocks = 0;

		if (monitor != null)
			monitor.beginTask(monitorTask, ProgressMonitor.UNKNOWN);

		LinkedHashMap<String, String> allFields = new LinkedHashMap<String, String>();
		try {
			while (true) {
				Message reply = read(true);
				if ("ProtocolError".equals(reply.type))
					throw new IOException("Protocol error");
				if (!freenetURI.equals(reply.field.get("Identifier")))
					continue; // identifier don't match, ignore it
				if ("IdentifierCollision".equals(reply.type))
					throw new IOException("IdentifierCollision");

				if ("SimpleProgress".equals(reply.type) && monitor != null) {
					if (totalBlocks == -1) {
						totalBlocks = Integer
								.parseInt(reply.field.get("Total"));
						monitor.beginTask(monitorTask, totalBlocks);
					}
					int tmp = Integer.parseInt(reply.field.get("Succeeded"));
					if (tmp < totalBlocks)
						monitor.update(tmp - completedBlocks);
					completedBlocks = tmp;
				}

				if (monitor != null && totalBlocks == -1)
					monitor.update(1);

				if ("URIGenerated".equals(reply.type))
					allFields.putAll(reply.field);
				if ("PutFailed".equals(reply.type)
						|| "PutSuccessful".equals(reply.type)
						|| "PutFetchable".equals(reply.type)) {
					allFields.putAll(reply.field);
					reply.field = allFields;
					return reply;
				}
			}
		} finally {
			if (monitor != null)
				monitor.endTask();
		}
	}

	static class GetResult {
		Map<String, String> field = new LinkedHashMap<String, String>();

		String uri;

		TemporaryBuffer data;
	}

	GetResult simpleGet(String freenetURI) throws IOException {
		GET_LOOP: for (;;) {
			final String rID = "GET-" + freenetURI;

			Message msg = new Message();
			msg.type = "ClientGet";
			msg.field.put("URI", freenetURI);
			msg.field.put("Identifier", rID);
			msg.field.put("PriorityClass", "1");
			msg.field.put("Verbosity", "1");
			msg.field.put("MaxSize", Integer.toString(Integer.MAX_VALUE));
			msg.field.put("Global", "false");
			msg.field.put("ClientToken", rID);
			msg.field.put("ReturnType", "direct");

			send(msg);

			GetResult ret = new GetResult();
			ret.uri = freenetURI;
			for (;;) {
				Message reply = read(true);
				if ("ProtocolError".equals(reply.type))
					throw new IOException("Protocol error");
				if (!rID.equals(reply.field.get("Identifier")))
					continue; // identifier don't match, ignore it
				if ("IdentifierCollision".equals(reply.type))
					throw new IOException("IdentifierCollision");

				if ("DataFound".equals(reply.type))
					ret.field.putAll(reply.field);
				if ("GetFailed".equals(reply.type)
						|| "AllData".equals(reply.type)) {
					final String rURI = reply.field.get("RedirectURI");
					if (rURI != null) {
						freenetURI = rURI;
						continue GET_LOOP;
					}

					ret.field.putAll(reply.field);
					ret.data = reply.extraData;
					return ret;
				}
			}
		}
	}

	/**
	 * Generate SSK Key Pair
	 * 
	 * @return the generated SSK key pair. <code>key[0]</code> is the public
	 *         key, <code>key[1]</code> is the private key.
	 * @throws IOException
	 *             if any I/O error occurred
	 */
	String[] generateSSK() throws IOException {
		Message msg = new Message();
		msg.type = "GenerateSSK";
		msg.field.put("Identifier", "GenerateSSK");

		send(msg);

		while (true) {
			Message reply = read(true);
			if ("ProtocolError".equals(reply.type))
				throw new IOException("Protocol error");

			if ("SSKKeypair".equals(reply.type)) {
				String[] keys = new String[2];
				keys[0] = reply.field.get("RequestURI");
				keys[1] = reply.field.get("InsertURI");
				return keys;
			}
		}
	}

	/**
	 * Get next FCP message
	 * 
	 * @param blocking
	 *            blocks when no data is available. NOTE: It may still block
	 *            even if <code>blocking</code> is <code>false</code>
	 * @return the FCP Message, or
	 *         <code>null<code> if no message available and <code>blocking</code>
	 *         is false.
	 * @throws IOException
	 *             if I/O error occur
	 */
	Message read(boolean blocking) throws IOException {
		if (!blocking && is.available() == 0)
			return null;

		Message msg = Message.parse(is);
		return msg;
	}

	void send(Message msg) throws IOException {
		msg.writeTo(os);
	}

	/**
	 * Close the connection
	 * 
	 * @throws IOException
	 *             if any I/O error occurred
	 */
	public void close() throws IOException {
		os.close();
		is.close();
		socket.close();
	}

	static class Message {
		String type;

		LinkedHashMap<String, String> field = new LinkedHashMap<String, String>();

		TemporaryBuffer extraData;

		Message() {
			// default constructor
		}

		static Message parse(InputStream in) throws IOException {
			Message ret = new Message();
			String line = readLine(in);
			ret.type = line;

			while (true) {
				line = readLine(in);

				if (line == null)
					throw new IOException("Malformed FCP message");
				if ("EndMessage".equals(line))
					break;
				if ("Data".equals(line)) {
					String strLen = ret.field.get("DataLength");
					if (strLen == null)
						throw new IOException("DataLength not found");
					int len;
					try {
						len = Integer.parseInt(strLen);
					} catch (NumberFormatException e) {
						throw new IOException("DataLength malformed");
					}
					ret.extraData = readData(in, len);
					break;
				}

				String[] v = line.split("=", 2);
				if (v.length != 2)
					throw new IOException("No '=' found in: " + line);
				ret.field.put(v[0], v[1]);
			}

			return ret;
		}

		void writeTo(OutputStream os) throws IOException {
			os.write(type.getBytes("UTF-8"));
			os.write('\n');

			for (Map.Entry<String, String> e : field.entrySet()) {
				String l = e.getKey() + '=' + e.getValue();
				os.write(l.getBytes("UTF-8"));
				os.write('\n');
			}

			if (extraData == null)
				os.write("EndMessage\n".getBytes("UTF-8"));
			else {
				os.write("Data\n".getBytes("UTF-8"));
				extraData.writeTo(os, null);
			}
			os.flush();
		}

		@Override
		public String toString() {
			return type + ":" + field;
		}

		static String readLine(InputStream in) throws IOException {
			byte[] buf = new byte[256];
			int offset = 0;

			while (true) {
				int b = in.read();
				if (b == -1)
					return null;
				if (b == '\n') {
					if (offset == 0)
						continue; // skip empty line
					break;
				}

				if (offset == buf.length) {
					if (offset >= 4096)
						throw new IOException("line too long");

					byte[] buf2 = new byte[buf.length * 2];
					System.arraycopy(buf, 0, buf2, 0, buf.length);
					buf = buf2;
				}

				buf[offset++] = (byte) b;
			}

			return new String(buf, 0, offset, "UTF-8");
		}

		static TemporaryBuffer readData(InputStream in, int len) throws IOException {
			TemporaryBuffer buf = new TemporaryBuffer();
			byte[] tmp = new byte[8192];
			int read = 0;
			while (read < len) {
				int r = in.read(tmp, 0, Math.min(tmp.length, len - read));
				if (r == -1)
					throw new IOException("Not enough data");
				buf.write(tmp, 0, r);
				read += r;
			}
			buf.close();
			return buf;
		}
	}
}
