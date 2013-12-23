package com.tencent.urs.tdbank.msg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;

import org.xerial.snappy.Snappy;

/**
 * 
 * @author steventian
 * 
 */
public class TDMsg {
	private static final int DEFAULT_CAPACITY = 4096;
	private final int CAPACITY;

	private final static byte[] MAGIC1 = { (byte) 0xf, (byte) 0x0 };
	// with timestamp
	private final static byte[] MAGIC2 = { (byte) 0xf, (byte) 0x1 };

	private LinkedHashMap<String, DataOutputBuffer> attr2MsgBuffer;
	private int datalen = 0;
	private int msgcnt = 0;
	private boolean compress;

	public static TDMsg newTDMsg(boolean compress) {
		return newTDMsg(DEFAULT_CAPACITY, compress);
	}

	public static TDMsg newTDMsg() {
		return newTDMsg(true);
	}

	public static TDMsg newTDMsg(int capacity, boolean compress) {
		return new TDMsg(capacity, compress);
	}

	// for create
	private TDMsg(int capacity, boolean compress) {
		this.compress = compress;
		CAPACITY = capacity;
		attr2MsgBuffer = new LinkedHashMap<String, DataOutputBuffer>();
	}

	public boolean addMsg(String attr, byte[] data) {
		return addMsg(attr, ByteBuffer.wrap(data));
	}

	/**
	 * return false means current msg is big enough, no other data should be
	 * added again, but attention: the input data has already been added, and if
	 * you add another data after return false it can also be added
	 * successfully.
	 * 
	 * @param attr
	 * @param data
	 * @param offset
	 * @param len
	 * @return
	 */
	public boolean addMsg(String attr, byte[] data, int offset, int len) {
		return addMsg(attr, ByteBuffer.wrap(data, offset, len));
	}

	public boolean addMsgs(String idtime, ByteBuffer data) {
		boolean res = true;
		Iterator<ByteBuffer> it = getIteratorBuffer(data);
		while (it.hasNext()) {
			res = this.addMsg(idtime, it.next());
		}
		return res;
	}

	public boolean addMsg(String attr, ByteBuffer data) {
		DataOutputBuffer outputBuffer = attr2MsgBuffer.get(attr);
		if (outputBuffer == null) {
			outputBuffer = new DataOutputBuffer();
			attr2MsgBuffer.put(attr, outputBuffer);
			this.datalen += attr.length() + 4 + 1;
		}
		int len = data.remaining();
		try {
			outputBuffer.writeInt(len);
			outputBuffer.write(data.array(), data.position(), len);
			this.datalen += len + 4;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		msgcnt++;
		return checkLen(attr, len);
	}

	private boolean checkLen(String attr, int len) {
		return datalen < CAPACITY;
	}

	public boolean isfull() {
		if (datalen >= CAPACITY) {
			return true;
		}
		return false;
	}

	public int getMsgCnt() {
		return msgcnt;
	}

	public ByteBuffer build() {
		try {
			byte[] tmpData = new byte[CAPACITY];
			DataOutputBuffer out = new DataOutputBuffer(CAPACITY);
			out.write(MAGIC2[0]);
			out.write(MAGIC2[1]);
			out.writeLong(System.currentTimeMillis());
			out.writeInt(attr2MsgBuffer.size());

			if (compress) {
				for (String attr : attr2MsgBuffer.keySet()) {
					out.writeUTF(attr);
					DataOutputBuffer data = attr2MsgBuffer.get(attr);
					int guessLen = Snappy.maxCompressedLength(data.getLength());
					if (tmpData.length < guessLen) {
						tmpData = new byte[guessLen];
					}
					int len = Snappy.compress(data.getData(), 0,
							data.getLength(), tmpData, 0);
					out.writeInt(len + 1);
					out.writeBoolean(compress);
					out.write(tmpData, 0, len);
				}
			} else {
				for (String attr : attr2MsgBuffer.keySet()) {
					out.writeUTF(attr);
					DataOutputBuffer data = attr2MsgBuffer.get(attr);
					out.writeInt(data.getLength() + 1);
					out.writeBoolean(compress);
					out.write(data.getData(), 0, data.getLength());
				}
			}
			out.write(MAGIC2[0]);
			out.write(MAGIC2[1]);
			out.close();
			return ByteBuffer.wrap(out.getData(), 0, out.getLength());
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public byte[] buildArray() {
		ByteBuffer buffer = this.build();
		if (buffer == null) {
			return null;
		}
		byte[] res = new byte[buffer.remaining()];
		System.arraycopy(buffer.array(), buffer.position(), res, 0, res.length);
		return res;
	}

	public void reset() {
		this.attr2MsgBuffer.clear();
		this.datalen = 0;
		msgcnt = 0;
	}

	private int attrcnt = -1;
	private LinkedHashMap<String, ByteBuffer> attr2Rawdata = null;
	private LinkedHashMap<String, Integer> attr2index = null;
	private long createtime = -1;

	// for parsed
	private TDMsg(ByteBuffer buffer) throws IOException {

		int magic = getMagic(buffer);
		if (magic == -1) {
			throw new IOException("not a tdmsg ... ");
		}

		CAPACITY = 0;
		DataInputBuffer input = new DataInputBuffer();
		input.reset(buffer.array(), buffer.position() + 2, buffer.remaining());
		if (magic >= 2) {
			createtime = input.readLong();
		}

		attrcnt = input.readInt();
		attr2Rawdata = new LinkedHashMap<String, ByteBuffer>(attrcnt);
		attr2index = new LinkedHashMap<String, Integer>(attrcnt);
		for (int i = 0; i < attrcnt; i++) {
			String attr = input.readUTF();
			int len = input.readInt();
			int pos = input.getPosition();
			attr2Rawdata.put(attr, ByteBuffer.wrap(input.getData(), pos, len));
			attr2index.put(attr, i);
			input.skip(len);
		}
	}

	private int getMagic(ByteBuffer buffer) {
		byte[] array = buffer.array();
		if (buffer.remaining() < 4) {
			return -1;
		}
		int pos = buffer.position();
		int rem = buffer.remaining();
		if (array[pos] == MAGIC2[0] && array[pos + 1] == MAGIC2[1]
				&& array[pos + rem - 2] == MAGIC2[0]
				&& array[pos + rem - 1] == MAGIC2[1]) {
			return 2;
		}
		if (array[pos] == MAGIC1[0] && array[pos + 1] == MAGIC1[1]
				&& array[pos + rem - 2] == MAGIC1[0]
				&& array[pos + rem - 1] == MAGIC1[1]) {
			return 1;
		}
		return -1;
	}

	public static TDMsg parseFrom(byte[] data) {
		return parseFrom(ByteBuffer.wrap(data));
	}

	public static TDMsg parseFrom(ByteBuffer buffer) {
		try {
			return new TDMsg(buffer);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public int getAttrCount() {
		return attrcnt;
	}

	public Set<String> getAttrs() {
		return this.attr2Rawdata.keySet();
	}

	public byte[] getRawData(String attr) {
		ByteBuffer buffer = getRawDataBuffer(attr);
		byte[] data = new byte[buffer.remaining()];
		System.arraycopy(buffer.array(), buffer.position(), data, 0,
				buffer.remaining());
		return data;
	}

	public int getIdx(String attr) {
		return this.attr2index.get(attr);
	}

	public ByteBuffer getRawDataBuffer(String attr) {
		return this.attr2Rawdata.get(attr);
	}

	public Iterator<byte[]> getIterator(String attr) {
		return getIterator(this.attr2Rawdata.get(attr));
	}

	public Iterator<ByteBuffer> getIteratorBuffer(String attr) {
		return getIteratorBuffer(this.attr2Rawdata.get(attr));
	}

	public static Iterator<byte[]> getIterator(byte[] rawdata) {
		return getIterator(ByteBuffer.wrap(rawdata));
	}

	public static Iterator<byte[]> getIterator(ByteBuffer rawdata) {
		try {
			final DataInputBuffer input = new DataInputBuffer();
			int compress = rawdata.get();
			if (compress == 1) {
				byte[] uncompressdata = new byte[Snappy.uncompressedLength(
						rawdata.array(), rawdata.position(),
						rawdata.remaining())];
				int len = Snappy.uncompress(rawdata.array(),
						rawdata.position(), rawdata.remaining(),
						uncompressdata, 0);
				input.reset(uncompressdata, len);
			} else {
				input.reset(rawdata.array(), rawdata.position(),
						rawdata.remaining());
			}

			return new Iterator<byte[]>() {

				@Override
				public boolean hasNext() {
					try {
						return input.available() > 0;
					} catch (IOException e) {
						e.printStackTrace();
					}
					return false;
				}

				@Override
				public byte[] next() {
					try {
						int len;
						len = input.readInt();
						byte[] res = new byte[len];
						input.read(res);
						return res;
					} catch (IOException e) {
						e.printStackTrace();
					}
					return null;
				}

				@Override
				public void remove() {
					this.next();
				}
			};
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	public static Iterator<ByteBuffer> getIteratorBuffer(byte[] rawdata) {
		return getIteratorBuffer(ByteBuffer.wrap(rawdata));
	}

	public static Iterator<ByteBuffer> getIteratorBuffer(ByteBuffer rawdata) {

		try {
			final DataInputBuffer input = new DataInputBuffer();
			int compress = rawdata.get();
			if (compress == 1) {
				byte[] uncompressdata = new byte[Snappy.uncompressedLength(
						rawdata.array(), rawdata.position(),
						rawdata.remaining())];
				int len = Snappy.uncompress(rawdata.array(),
						rawdata.position(), rawdata.remaining(),
						uncompressdata, 0);
				input.reset(uncompressdata, len);
			} else {
				input.reset(rawdata.array(), rawdata.position(),
						rawdata.remaining());
			}

			final byte[] uncompressdata = input.getData();

			return new Iterator<ByteBuffer>() {

				@Override
				public boolean hasNext() {
					try {
						return input.available() > 0;
					} catch (IOException e) {
						e.printStackTrace();
					}
					return false;
				}

				@Override
				public ByteBuffer next() {
					try {
						int len = input.readInt();
						int pos = input.getPosition();
						input.skip(len);
						return ByteBuffer.wrap(uncompressdata, pos, len);
					} catch (IOException e) {
						e.printStackTrace();
					}
					return null;
				}

				@Override
				public void remove() {
					this.next();
				}
			};
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	public long getCreatetime() {
		return createtime;
	}

}
