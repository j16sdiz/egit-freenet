/*
 *  Copyright (C) 2008  Shawn Pearce <spearce@spearce.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public
 *  License, version 2, as published by the Free Software Foundation.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
 */
package org.spearce.jgit.treewalk;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Comparator;

import org.spearce.jgit.errors.CorruptObjectException;
import org.spearce.jgit.lib.Constants;
import org.spearce.jgit.lib.FileMode;

/**
 * Walks a working directory tree as part of a {@link TreeWalk}.
 * <p>
 * Most applications will want to use the standard implementation of this
 * iterator, {@link FileTreeIterator}, as that does all IO through the standard
 * <code>java.io</code> package. Plugins for a Java based IDE may however wish
 * to create their own implementations of this class to allow traversal of the
 * IDE's project space, as well as benefit from any caching the IDE may have.
 * 
 * @see FileTreeIterator
 */
public abstract class WorkingTreeIterator extends AbstractTreeIterator {
	/** An empty entry array, suitable for return from {@link #getEntries()}. */
	protected static final Entry[] EOF = {};

	/** Size we perform file IO in if we have to read and hash a file. */
	private static final int BUFFER_SIZE = 2048;

	/** The {@link #idBuffer()} for the current entry. */
	private byte[] contentId;

	/** Value of {@link #ptr} when {@link #contentId} was last populated. */
	private int contentIdFromPtr;

	/** Buffer used to perform {@link #contentId} computations. */
	private byte[] contentReadBuffer;

	/** Digest computer for {@link #contentId} computations. */
	private MessageDigest contentDigest;

	/** File name character encoder. */
	private final CharsetEncoder nameEncoder;

	/** List of entries obtained from the subclass. */
	private Entry[] entries;

	/** Total number of entries in {@link #entries} that are valid. */
	private int entryCnt;

	/** Current position within {@link #entries}. */
	private int ptr;

	/** Create a new iterator with no parent. */
	protected WorkingTreeIterator() {
		super();
		nameEncoder = Constants.CHARSET.newEncoder();
	}

	/**
	 * Create an iterator for a subtree of an existing iterator.
	 * 
	 * @param p
	 *            parent tree iterator.
	 */
	protected WorkingTreeIterator(final WorkingTreeIterator p) {
		super(p);
		nameEncoder = p.nameEncoder;
	}

	@Override
	protected byte[] idBuffer() {
		if (contentIdFromPtr == ptr - 1)
			return contentId;
		if (entries == EOF)
			return zeroid;

		switch (mode & 0170000) {
		case 0100000: /* normal files */
			contentIdFromPtr = ptr - 1;
			return contentId = idBufferBlob(entries[contentIdFromPtr]);
		case 0120000: /* symbolic links */
			// Java does not support symbolic links, so we should not
			// have reached this particular part of the walk code.
			//
			return zeroid;
		case 0160000: /* gitlink */
			// TODO: Support obtaining current HEAD SHA-1 from nested repository
			//
			return zeroid;
		}
		return zeroid;
	}

	private void initializeDigest() {
		if (contentDigest != null)
			return;

		if (parent == null) {
			contentReadBuffer = new byte[BUFFER_SIZE];
			contentDigest = Constants.newMessageDigest();
		} else {
			final WorkingTreeIterator p = (WorkingTreeIterator) parent;
			p.initializeDigest();
			contentReadBuffer = p.contentReadBuffer;
			contentDigest = p.contentDigest;
		}
	}

	private static final byte[] digits = { '0', '1', '2', '3', '4', '5', '6',
			'7', '8', '9' };

	private static final String TYPE_BLOB = Constants.TYPE_BLOB;

	private static final byte[] hblob = Constants.encodeASCII(TYPE_BLOB);

	private byte[] idBufferBlob(final Entry e) {
		try {
			final InputStream is = e.openInputStream();
			if (is == null)
				return zeroid;
			try {
				initializeDigest();

				contentDigest.reset();
				contentDigest.update(hblob);
				contentDigest.update((byte) ' ');

				final long blobLength = e.getLength();
				long sz = blobLength;
				if (sz == 0) {
					contentDigest.update((byte) '0');
				} else {
					final int bufn = contentReadBuffer.length;
					int p = bufn;
					do {
						contentReadBuffer[--p] = digits[(int) (sz % 10)];
						sz /= 10;
					} while (sz > 0);
					contentDigest.update(contentReadBuffer, p, bufn - p);
				}
				contentDigest.update((byte) 0);

				for (;;) {
					final int r = is.read(contentReadBuffer);
					if (r <= 0)
						break;
					contentDigest.update(contentReadBuffer, 0, r);
					sz += r;
				}
				if (sz != blobLength)
					return zeroid;
				return contentDigest.digest();
			} finally {
				try {
					is.close();
				} catch (IOException err2) {
					// Suppress any error related to closing an input
					// stream. We don't care, we should not have any
					// outstanding data to flush or anything like that.
				}
			}
		} catch (IOException err) {
			// Can't read the file? Don't report the failure either.
			//
			return zeroid;
		}
	}

	@Override
	protected int idOffset() {
		return 0;
	}

	@Override
	public boolean eof() {
		return entries == EOF;
	}

	@Override
	public void next() throws CorruptObjectException {
		if (entries == null)
			loadEntries();
		if (ptr == entryCnt) {
			entries = EOF;
			return;
		}
		if (entries == EOF)
			return;

		final Entry e = entries[ptr++];
		mode = e.getMode().getBits();

		final int nameLen = e.encodedNameLen;
		while (pathOffset + nameLen > path.length)
			growPath(pathOffset);
		System.arraycopy(e.encodedName, 0, path, pathOffset, nameLen);
		pathLen = pathOffset + nameLen;
	}

	private static final Comparator<Entry> ENTRY_CMP = new Comparator<Entry>() {
		public int compare(final Entry o1, final Entry o2) {
			final byte[] a = o1.encodedName;
			final byte[] b = o2.encodedName;
			final int aLen = o1.encodedNameLen;
			final int bLen = o2.encodedNameLen;
			int cPos;

			for (cPos = 0; cPos < aLen && cPos < bLen; cPos++) {
				final int cmp = (a[cPos] & 0xff) - (b[cPos] & 0xff);
				if (cmp != 0)
					return cmp;
			}

			if (cPos < aLen) {
				final int aj = a[cPos] & 0xff;
				final int lastb = lastPathChar(o2);
				if (aj < lastb)
					return -1;
				else if (aj > lastb)
					return 1;
				else if (cPos == aLen - 1)
					return 0;
				else
					return -1;
			}

			if (cPos < bLen) {
				final int bk = b[cPos] & 0xff;
				final int lasta = lastPathChar(o1);
				if (lasta < bk)
					return -1;
				else if (lasta > bk)
					return 1;
				else if (cPos == bLen - 1)
					return 0;
				else
					return 1;
			}

			final int lasta = lastPathChar(o1);
			final int lastb = lastPathChar(o2);
			if (lasta < lastb)
				return -1;
			else if (lasta > lastb)
				return 1;

			if (aLen == bLen)
				return 0;
			else if (aLen < bLen)
				return -1;
			else
				return 1;
		}
	};

	static int lastPathChar(final Entry e) {
		return e.getMode() == FileMode.TREE ? '/' : '\0';
	}

	private void loadEntries() throws CorruptObjectException {
		// Filter out nulls, . and .. as these are not valid tree entries,
		// also cache the encoded forms of the path names for efficient use
		// later on during sorting and iteration.
		//
		try {
			entries = getEntries();
			int i, o;

			for (i = 0, o = 0; i < entries.length; i++) {
				final Entry e = entries[i];
				if (e == null)
					continue;
				final String name = e.getName();
				if (".".equals(name) || "..".equals(name))
					continue;
				if (parent == null && ".git".equals(name))
					continue;
				if (i != o)
					entries[o] = e;
				e.encodeName(nameEncoder);
				o++;
			}
			entryCnt = o;
			contentIdFromPtr = -1;
			Arrays.sort(entries, 0, entryCnt, ENTRY_CMP);
		} catch (CharacterCodingException e) {
			final CorruptObjectException why;
			why = new CorruptObjectException("Invalid file name encoding");
			why.initCause(e);
			throw why;
		} catch (IOException e) {
			final CorruptObjectException why;
			why = new CorruptObjectException("Error reading directory");
			why.initCause(e);
			throw why;
		}
	}

	/**
	 * Obtain the current entry from this iterator.
	 * 
	 * @return the currently selected entry.
	 */
	protected Entry current() {
		return entries[ptr - 1];
	}

	/**
	 * Obtain an unsorted list of this iterator's contents.
	 * <p>
	 * Implementations only need to provide the unsorted contents of their lower
	 * level directory. The caller will automatically prune out ".", "..",
	 * ".git", as well as null entries as necessary, and then sort the array
	 * for iteration within a TreeWalk instance.
	 * <p>
	 * The returned array will be modified by the caller.
	 * <p>
	 * This method is only invoked once per iterator instance.
	 * 
	 * @return unsorted list of the immediate children. Never null, but may be
	 *         {@link #EOF} if no items can be obtained.
	 * @throws IOException
	 *             reading the contents failed due to IO errors.
	 */
	protected abstract Entry[] getEntries() throws IOException;

	/** A single entry within a working directory tree. */
	protected static abstract class Entry {
		byte[] encodedName;

		int encodedNameLen;

		void encodeName(final CharsetEncoder enc)
				throws CharacterCodingException {
			final ByteBuffer b = enc.encode(CharBuffer.wrap(getName()));
			encodedNameLen = b.limit();
			if (b.hasArray())
				encodedName = b.array();
			else
				b.get(encodedName = new byte[encodedNameLen]);
		}

		public String toString() {
			return getMode().toString() + " " + getName();
		}

		/**
		 * Get the type of this entry.
		 * <p>
		 * <b>Note: Efficient implementation required.</b>
		 * <p>
		 * The implementation of this method must be efficient. If a subclass
		 * needs to compute the value they should cache the reference within an
		 * instance member instead.
		 * 
		 * @return a file mode constant from {@link FileMode}.
		 */
		public abstract FileMode getMode();

		/**
		 * Get the byte length of this entry.
		 * <p>
		 * <b>Note: Efficient implementation required.</b>
		 * <p>
		 * The implementation of this method must be efficient. If a subclass
		 * needs to compute the value they should cache the reference within an
		 * instance member instead.
		 * 
		 * @return size of this file, in bytes.
		 */
		public abstract long getLength();

		/**
		 * Get the name of this entry within its directory.
		 * <p>
		 * Efficient implementations are not required. The caller will obtain
		 * the name only once and cache it once obtained.
		 * 
		 * @return name of the entry.
		 */
		public abstract String getName();

		/**
		 * Obtain an input stream to read the file content.
		 * <p>
		 * Efficient implementations are not required. The caller will usually
		 * obtain the stream only once per entry, if at all.
		 * <p>
		 * The input stream should not use buffering if the implementation can
		 * avoid it. The caller will buffer as necessary to perform efficient
		 * block IO operations.
		 * <p>
		 * The caller will close the stream once complete.
		 * 
		 * @return a stream to read from the file.
		 * @throws IOException
		 *             the file could not be opened for reading.
		 */
		public abstract InputStream openInputStream() throws IOException;
	}
}