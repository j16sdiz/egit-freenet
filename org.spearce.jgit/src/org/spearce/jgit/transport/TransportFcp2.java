/*
 * Copyright (C) 2009, CHENG Yuk-Pong, Daniel <j16sdiz+freenet@gmail.com>
 * Copyright (C) 2008, Shawn O. Pearce <spearce@spearce.org>
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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.spearce.jgit.errors.NotSupportedException;
import org.spearce.jgit.errors.PackProtocolException;
import org.spearce.jgit.errors.TransportException;
import org.spearce.jgit.lib.Constants;
import org.spearce.jgit.lib.ObjectId;
import org.spearce.jgit.lib.ProgressMonitor;
import org.spearce.jgit.lib.Ref;
import org.spearce.jgit.lib.Repository;
import org.spearce.jgit.lib.Ref.Storage;
import org.spearce.jgit.transport.FreenetFCP.GetResult;
import org.spearce.jgit.transport.FreenetFCP.Message;
import org.spearce.jgit.util.FS;
import org.spearce.jgit.util.TemporaryBuffer;

/**
 * Transport over Freenet Client Protocol 2.0
 *<p>
 * URI are in forms of <code>freenet://USK@PUBLIC_KEY/identifier</code>
 * (read-only) or <code>freenet://<config file name>/identifier</code>. The
 * identifier must not contain any slash.
 * <p>
 * Example configuration file (in <code>~/</code> or <code>./.git/</code>):
 *
 * <pre>
 * publicKey=USK@...............,....,AQACAAE/
 * privateKey=USK@..............,....,AQECAAE/
 * </pre>
 *
 * @see WalkFetchConnection
 */
class TransportFcp2 extends Transport implements WalkTransport {
	static boolean canHandle(final URIish uri) {
		if (!uri.isRemote())
			return false;
		final String s = uri.getScheme();
		return "freenet".equals(s);
	}

	private final String publicKey;

	private final String privateKey;

	private FreenetFCP fcp;

	TransportFcp2(final Repository local, final URIish uri)
			throws NotSupportedException {
		super(local, uri);

		File propsFile = new File(local.getDirectory(), uri.getHost());
		if (!propsFile.isFile())
			propsFile = new File(FS.userHome(), uri.getHost());
		if (propsFile.isFile()) {
			Properties prop;
			try {
				final FileInputStream in = new FileInputStream(propsFile);
				prop = new Properties();
				prop.load(in);
			} catch (final IOException e) {
				throw new NotSupportedException("cannot read " + propsFile, e);
			}

			String p = uri.getPath();
			if (p.startsWith("/"))
				p = p.substring(1);
			publicKey = prop.getProperty("publicKey") + p;
			privateKey = prop.getProperty("privateKey") + p;
		} else {
			publicKey = uri.toString().substring(10); // Remove 'freenet://' prefix
			privateKey = null;
		}
	}

	@Override
	public FetchConnection openFetch() throws TransportException {
		try {
			fcp = new FreenetFCP();
			fcp.connect();
			fcp.hello("JGit-" + toString());

			final FreenetDB c = new FreenetDB(fcp, publicKey, privateKey);
			final WalkFetchConnection r = new WalkFetchConnection(this, c);
			r.available(c.readAdvertisedRefs());
			return r;
		} catch (final IOException e) {
			throw new TransportException("IO Error" + e, e);
		}
	}

	@Override
	public PushConnection openPush() throws TransportException {
		try {
			fcp = new FreenetFCP();
			fcp.connect();
			fcp.hello("JGit-" + toString());

			final FreenetDB c = new FreenetDB(fcp, publicKey, privateKey);
			final WalkPushConnection r = new WalkPushConnection(this, c) {
				@Override
				public void push(final ProgressMonitor monitor,
						final Map<String, RemoteRefUpdate> refUpdates)
						throws TransportException {
					super.push(monitor, refUpdates);
					try {
						c.commit(monitor, "Commiting transection");
					} catch (IOException e) {
						throw new TransportException("FCP Error", e);
					}
				}
			};
			r.available(c.readAdvertisedRefs());
			return r;
		} catch (final IOException e) {
			throw new TransportException("IO Error" + e, e);
		}
	}

	@Override
	public void close() {
		try {
			if (fcp != null)
				fcp.close();
		} catch (final IOException e) {
			// Fall through.
		}
		fcp = null;
	}

	static class FreenetDB extends WalkRemoteObjectDatabase {
		private static final String URI_DELETED = "[DELETED]";

		protected static final String NOT_IN_ARCHIVE = "10";

		protected static final String DATA_NOT_FOUND = "13";

		protected static final String FILELIST = ".JGIT-FREENET-FILELIST";

		protected final FreenetFCP conn;

		/** Public key as specified by user */
		protected final String publicKey;

		/** Public key after resolving the USK@ edition */
		protected final String currentKey;

		protected final String privateKey;

		protected final SortedMap<String, String> fileList;

		protected final SortedMap<String, TemporaryBuffer> smallFile;

		protected final Set<TemporaryBuffer> tmpBuffers;

		protected String baseArchive;

		/**
		 * Create a new freesite
		 *
		 * @param conn
		 *            freenet fcp connection
		 * @param publicKey
		 *            public key
		 * @param privateKey
		 *            private key, may be <code>null</code>.
		 * @throws IOException
		 */
		public FreenetDB(final FreenetFCP conn, final String publicKey,
				final String privateKey) throws IOException {
			this.conn = conn;
			this.fileList = new TreeMap<String, String>();
			this.smallFile = new TreeMap<String, TemporaryBuffer>();
			this.tmpBuffers = new HashSet<TemporaryBuffer>();

			/*-
			 * Freenet URI Format:
			 *   USK@XXXXXXXXXX,XXXXX,XXXX/BLAR/100/HAHA/LALA
			 *   <--><--------------------><--> <-> <------->
			 *    (1)         (2)           (3) (4)    (5)
			 *   SSK@XXXXXXXXXX,XXXXX,XXXX/BLAR/HAHA/LALA
			 *   <--><--------------------><--> <------->
			 *    (1)         (2)           (3)    (5)
			 *   CHK@XXXXXXXXXX,XXXXX,XXXX/HAHA/LALA
			 *   <--><--------------------><------->
			 *    (1)         (2)             (5)
			 *
			 *  1 - key type
			 *  2 - key
			 *  3 - docName
			 *  4 - edition number (only for USK@)
			 *  5 - metastring -- MUST NOT be specified
			 */
			if (publicKey != null) {
				if (!publicKey.startsWith("USK@")
						&& !publicKey.startsWith("SSK@"))
					throw new IllegalArgumentException("Invalid public key: "
							+ publicKey);
				if (!publicKey.endsWith("/"))
					throw new IllegalArgumentException(
							"Invalid p key, missing tailing slash: "
									+ publicKey);
				if (privateKey != null && !privateKey.endsWith("/"))
					throw new IllegalArgumentException(
							"Invalid private key, missing tailing slash: "
									+ privateKey);

				this.publicKey = publicKey;
				this.currentKey = getCurrentKey(publicKey);
				this.privateKey = privateKey;

				loadFileList();
			} else {
				// new site
				final String s[] = conn.generateSSK();
				this.publicKey = s[0].replace("SSK@", "USK@") + ".git/0";
				this.currentKey = s[0] + ".git-0";
				this.privateKey = s[1].replace("SSK@", "USK@") + ".git/0";
			}
		}

		private String getCurrentKey(final String pubkey) throws IOException {
			if (pubkey.startsWith("SSK@") || pubkey.startsWith("CHK@")
					|| pubkey.startsWith("KSK@"))
				return pubkey;
			if (!pubkey.startsWith("USK@"))
				throw new IOException("Unknown key type: " + pubkey);

			/*-
			 *   USK@XXXXXXXXXX,XXXXX,XXXX/BLARBLAR/100000/....
			 *   <-----------p[0]--------> <-p[1]-> <p[2]> ....
			 */
			final String[] p = pubkey.split("\\/");
			if (p[2].startsWith("-"))
				p[2] = p[2].substring(1);
			final GetResult m = conn.simpleGet(p[0] + "/" + p[1] + "/-" + p[2]);
			if (m.data != null)
				tmpBuffers.add(m.data);
			if (!m.uri.startsWith("USK@")) // ugh?
				throw new IOException("Redirected to non-USK@: " + m.uri);

			final String[] q = m.uri.split("\\/");
			if (q[2].startsWith("-"))
				q[2] = q[2].substring(1);
			return "SSK@" + q[0].substring(4) + "/" + q[1] + "-" + q[2] + "/";
		}

		private void loadFileList() throws IOException {
			final GetResult m = conn.simpleGet(currentKey + FILELIST);

			if (m.data == null) {
				if (NOT_IN_ARCHIVE.equals(m.field.get("Code"))) {
					// the docName exist, just not the file list
					baseArchive = currentKey;
					for (final String p : getPackNames()) {
						final String pa = "pack/" + p;
						final String ba = p.substring(0, p.length() - 5);
						final String pi = "pack/" + ba + ".idx";

						fileList.put(pa, currentKey + "objects/" + pa);
						fileList.put(pi, currentKey + "objects/" + pi);
					}
				} else {
					if (DATA_NOT_FOUND.equals(m.field.get("Code"))) {
						// ignore this, maybe new site
					} else {
						throw new IOException(m.field.get("CodeDescription")
								+ "(" + m.field.get("Code") + "): " + m.uri
								+ " : " + m.field.get("ExtraDescription"));
					}
				}
				return;
			}
			tmpBuffers.add(m.data);

			final BufferedReader br = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(m.data.toByteArray()), "UTF-8"));
			try {
				for (;;) {
					final String line = br.readLine();
					if (line == null)
						break;

					if (line.startsWith("#"))
						continue;
					if (line.startsWith("^") && !line.contains("\0")) {
						baseArchive = line.substring(1);
						continue;
					}

					final int idx = line.lastIndexOf('\t');
					if (idx == -1)
						continue;

					final String k = line.substring(0, idx);
					final String v = line.substring(idx + 1);
					fileList.put(k, "*".equals(v) ? currentKey + k : v);
				}
			} finally {
				br.close();
			}
		}

		/**
		 * Commit the changes
		 *
		 * @param monitor
		 *            (optional) progress monitor to post write completion to
		 *            during the stream's close method.
		 * @param monitorTask
		 *            (optional) task name to display during the close method.
		 * @throws IOException
		 */
		public synchronized void commit(final ProgressMonitor monitor,
				final String monitorTask) throws IOException {
			if (privateKey == null)
				return;

			final TemporaryBuffer tmpBuf = new TemporaryBuffer();
			long fileListSize;
			{
				final StringBuffer w = new StringBuffer();
				w.append("# File List\n");
				if (baseArchive != null) {
					w.append('^');
					w.append(baseArchive);
					w.append('\n');
				}
				for (final Map.Entry<String, String> e : fileList.entrySet()) {
					w.append(e.getKey());
					w.append('\t');
					w.append(e.getValue());
					w.append('\n');
				}
				for (final String f : smallFile.keySet()) {
					w.append(f);
					w.append("\t*\n");
				}
				byte[] b = w.toString().getBytes("UTF-8");
				fileListSize = b.length;
				tmpBuf.write(b);
			}
			for (final TemporaryBuffer b : smallFile.values())
				b.writeTo(tmpBuf, null);

			final Message msg = new Message();
			msg.type = "ClientPutComplexDir";
			msg.field.put("Identifier", privateKey);
			msg.field.put("URI", privateKey);
			msg.field.put("Verbosity", monitor == null ? "0" : "1");
			msg.field.put("PriorityClass", "1");
			msg.field.put("EarlyEncode", "true"); // progress
			msg.field.put("Global", "false");
			msg.field.put("ClientToken", privateKey);
			msg.field.put("Persistence", "connection");
			msg.field.put("DefaultName", FILELIST);

			msg.field.put("Files.0.Name", FILELIST);
			msg.field.put("Files.0.UploadFrom", "direct");
			msg.field.put("Files.0.DataLength", Long.toString(fileListSize));
			msg.field.put("Files.0.Metadata.ContentType", "text/plain");

			int idx = 1;
			for (final Map.Entry<String, TemporaryBuffer> e : smallFile
					.entrySet()) {
				msg.field.put("Files." + idx + ".Name", e.getKey());
				msg.field.put("Files." + idx + ".UploadFrom", "direct");
				msg.field.put("Files." + idx + ".DataLength", //
						Long.toString(e.getValue().length()));
				idx++;
			}
			for (final Map.Entry<String, String> e : fileList.entrySet()) {
				if (URI_DELETED.equals(e.getValue()))
					continue;
				msg.field.put("Files." + idx + ".Name", e.getKey());
				msg.field.put("Files." + idx + ".UploadFrom", "redirect");
				msg.field.put("Files." + idx + ".TargetURI", e.getValue());
				idx++;
			}

			for (final TemporaryBuffer tmp2 : smallFile.values())
				tmp2.destroy();
			smallFile.clear();

			tmpBuf.close();
			msg.extraData = tmpBuf;
			conn.send(msg);
			tmpBuf.destroy();

			int totalBlocks = -1;
			int completedBlocks = 0;
			if (monitor != null)
				monitor.beginTask(monitorTask, ProgressMonitor.UNKNOWN);
			try {
				while (true) {
					final Message r = conn.read(true);
					if ("SimpleProgress".equals(r.type) && monitor != null) {
						if (totalBlocks == -1) {
							totalBlocks = Integer
									.parseInt(r.field.get("Total"));
							monitor.beginTask(monitorTask, totalBlocks);
						}
						final int tmp = Integer.parseInt(r.field
								.get("Succeeded"));
						if (tmp < totalBlocks)
							monitor.update(tmp - completedBlocks);
						completedBlocks = tmp;
					}

					if ("PutFailed".equals(r.type))
						throw new IOException("FCP Error: " + r);

					if ("PutSuccessful".equals(r.type)
							|| "PutFetchable".equals(r.type))
						return;
				}
			} finally {
				if (monitor != null)
					monitor.endTask();
			}
		}

		@Override
		Collection<String> getPackNames() throws IOException {
			final Collection<String> packs = new ArrayList<String>();
			try {
				final BufferedReader br = openReader(INFO_PACKS);
				try {
					for (;;) {
						final String s = br.readLine();
						if (s == null || s.length() == 0)
							break;
						if (!s.startsWith("P pack-") || !s.endsWith(".pack"))
							throw new PackProtocolException(
									"invalid advertisement of " + s);
						packs.add(s.substring(2));
					}
					return packs;
				} finally {
					br.close();
				}
			} catch (final FileNotFoundException err) {
				return packs;
			}
		}

		@Override
		URIish getURI() {
			try {
				return new URIish("freenet://" + currentKey);
			} catch (final URISyntaxException e) {
				return null;
			}
		}

		@Override
		FileStream open(String path) throws FileNotFoundException, IOException {
			path = resolvePath(path);

			// small file
			final TemporaryBuffer b = smallFile.get(path);
			if (b != null)
				return new FileStream(new ByteArrayInputStream(b.toByteArray()));

			// in file list
			final String rURI = fileList.get(path);
			if (URI_DELETED.equals(rURI))
				throw new FileNotFoundException("deleted");
			if (rURI != null) {
				final GetResult r = conn.simpleGet(rURI);
				if (r.data == null)
					throw new IOException("FCP Error: "
							+ r.field.get("CodeDescription") + "("
							+ r.field.get("Code") + "): " + r.uri + " : "
							+ r.field.get("ExtraDescription"));
				tmpBuffers.add(r.data);
				return new FileStream(r.data.getInputStream());
			}

			if (baseArchive != null) {
				final GetResult r = conn.simpleGet(baseArchive + path);
				if (r.data == null) {
					if (NOT_IN_ARCHIVE.equals(r.field.get("Code")))
						throw new FileNotFoundException();
					throw new IOException("FCP Error: "
							+ r.field.get("CodeDescription") + "("
							+ r.field.get("Code") + "): " + r.uri + " : "
							+ r.field.get("ExtraDescription"));
				}
				tmpBuffers.add(r.data);
				fileList.put(path, r.uri);
				return new FileStream(r.data.getInputStream());
			}

			throw new FileNotFoundException();
		}

		@Override
		synchronized void deleteFile(final String path) throws IOException {
			String resolvedPath = resolvePath(path);
			
			checkWrite();
			smallFile.remove(resolvedPath);
			fileList.put(resolvedPath, URI_DELETED);
		}

		@Override
		OutputStream writeFile(final String path,
				final ProgressMonitor monitor, final String monitorTask)
				throws IOException {
			checkWrite();

			TemporaryBuffer tb = new TemporaryBuffer() {
				@Override
				public void close() throws IOException {
					super.close();
					insert(path, this, monitor, monitorTask);
				}
			};
			tmpBuffers.add(tb);
			return tb;
		}

		private synchronized void insert(final String path, TemporaryBuffer buf,
				final ProgressMonitor monitor, final String monitorTask)
				throws IOException {
			checkWrite();

			String resolvedPath = resolvePath(path);
			fileList.remove(resolvedPath);
			smallFile.remove(resolvedPath);

			if (buf.length() < 2048) {
				smallFile.put(resolvedPath, buf);
			} else {
				final Message r = conn.simplePut("CHK@", buf, monitor,
						monitorTask);
				if ("PutFailed".equals(r.type))
					throw new IOException("FCP PutFailed: " + r.field);
				fileList.put(resolvedPath, r.field.get("URI"));
			}
		}

		private String resolvePath(String path) {
			while (path.endsWith("/"))
				path = path.substring(0, path.length() - 1);
			while (path.startsWith("/"))
				path = path.substring(1);

			String k = "objects/";
			while (path.startsWith(ROOT_DIR)) {
				k = "";
				path = path.substring(ROOT_DIR.length());
			}

			return k + path;
		}

		private void checkWrite() throws IOException {
			if (privateKey == null)
				throw new IOException("No private key defined - read only");
			if (!privateKey.startsWith("USK@"))
				throw new IOException("Private key not USK@ - read only");
		}

		@Override
		Collection<WalkRemoteObjectDatabase> getAlternates() throws IOException {
			return null;
		}

		@Override
		WalkRemoteObjectDatabase openAlternate(final String location)
				throws IOException {
			throw new IOException("Open alternate '" + location
					+ "' not supported.");
		}

		Map<String, Ref> readAdvertisedRefs() throws TransportException {
			final TreeMap<String, Ref> avail = new TreeMap<String, Ref>();
			readInfoRefs(avail);
			readRef(avail, Constants.HEAD);
			return avail;
		}

		private Map<String, Ref> readInfoRefs(final TreeMap<String, Ref> avail)
				throws TransportException {
			try {
				final BufferedReader br = openReader(INFO_REFS);
				for (;;) {
					final String line = br.readLine();
					if (line == null)
						break;

					final int tab = line.indexOf('\t');
					if (tab < 0)
						throw invalidAdvertisement(line);

					String name;
					final ObjectId id;

					name = line.substring(tab + 1);
					id = ObjectId.fromString(line.substring(0, tab));
					if (name.endsWith("^{}")) {
						name = name.substring(0, name.length() - 3);
						final Ref prior = avail.get(name);
						if (prior == null)
							throw outOfOrderAdvertisement(name);

						if (prior.getPeeledObjectId() != null)
							throw duplicateAdvertisement(name + "^{}");

						avail.put(name, new Ref(Ref.Storage.NETWORK, name,
								prior.getObjectId(), id, true));
					} else {
						final Ref prior = avail.put(name, new Ref(
								Ref.Storage.NETWORK, name, id));
						if (prior != null)
							throw duplicateAdvertisement(name);
					}
				}
				return avail;
			} catch (final FileNotFoundException noRef) {
				return null;
			} catch (final IOException err) {
				throw new TransportException(INFO_REFS
						+ ": cannot read available refs", err);
			}
		}

		private Ref readRef(final TreeMap<String, Ref> avail, final String rn)
				throws TransportException {
			final String s;
			String ref = ROOT_DIR + rn;
			try {
				final BufferedReader br = openReader(ref);
				try {
					s = br.readLine();
				} finally {
					br.close();
				}
			} catch (FileNotFoundException noRef) {
				return null;
			} catch (IOException err) {
				throw new TransportException(getURI(), "read " + ref, err);
			}

			if (s == null)
				throw new TransportException(getURI(), "Empty ref: " + rn);

			if (s.startsWith("ref: ")) {
				final String target = s.substring("ref: ".length());
				Ref r = avail.get(target);
				if (r == null)
					r = readRef(avail, target);
				if (r == null)
					return null;
				r = new Ref(Storage.LOOSE_PACKED, rn, r.getObjectId(), r
						.getPeeledObjectId(), r.isPeeled());
				avail.put(r.getName(), r);
				return r;
			}

			if (ObjectId.isId(s)) {
				final Ref r = new Ref(Storage.LOOSE_PACKED, rn, ObjectId
						.fromString(s));
				avail.put(r.getName(), r);
				return r;
			}

			throw new TransportException(getURI(), "Bad ref: " + rn + ": " + s);
		}

		private PackProtocolException outOfOrderAdvertisement(final String n) {
			return new PackProtocolException("advertisement of " + n
					+ "^{} came before " + n);
		}

		private PackProtocolException duplicateAdvertisement(final String n) {
			return new PackProtocolException("duplicate advertisements of " + n);
		}

		private PackProtocolException invalidAdvertisement(final String n) {
			return new PackProtocolException("invalid advertisement of " + n);
		}

		@Override
		public String toString() {
			return "PUB: " + publicKey //
					+ "\nPRI: " + privateKey //
					+ "\nCUR: " + currentKey //
					+ "\n BA: " + baseArchive //
					+ "\n FL: " + fileList;
		}

		@Override
		void close() {
			for (TemporaryBuffer b : tmpBuffers)
				b.destroy();

			tmpBuffers.clear();
			smallFile.clear();
			fileList.clear();
		}
	}
}
