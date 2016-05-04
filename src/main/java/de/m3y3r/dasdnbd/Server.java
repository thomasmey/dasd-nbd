package de.m3y3r.dasdnbd;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server implements Runnable {

	private static final int PORT = 10809;

	/* global flags */
	private static final int NBD_FLAG_FIXED_NEWSTYLE = 1 << 0; /* new-style export that actually supports extending */
	private static final int NBD_FLAG_NO_ZEROES = 1 << 1; /* we won't send the 128 bits of zeroes if the client sends NBD_FLAG_C_NO_ZEROES */

	/* Options that the client can select to the server */
	private static final int NBD_OPT_EXPORT_NAME = 1; /** Client wants to select a named export (is followed by name of export) */
	private static final int NBD_OPT_ABORT = 2; /** Client wishes to abort negotiation */
	private static final int NBD_OPT_LIST = 3;

	/* values for transmission flags field */
	private static final int NBD_FLAG_HAS_FLAGS = (1 << 0); /* Flags are there */
	private static final int NBD_FLAG_READ_ONLY = (1 << 1); /* Device is read-only */
	private static final int NBD_FLAG_SEND_FLUSH = (1 << 2); /* Send FLUSH */
	private static final int NBD_FLAG_SEND_FUA = (1 << 3); /* Send FUA (Force Unit Access) */
	private static final int NBD_FLAG_ROTATIONAL = (1 << 4); /* Use elevator algorithm - rotational media */
	private static final int NBD_FLAG_SEND_TRIM = (1 << 5); /* Send TRIM (discard) */

	private static final int NBD_CMD_READ = 0;
	private static final int NBD_CMD_WRITE = 1;
	private static final int NBD_CMD_DISC = 2;
	private static final int NBD_CMD_FLUSH = 3;
	private static final int NBD_CMD_TRIM = 4;

	private static final int NBD_REP_FLAG_ERROR = (1 << 31); /** If the high bit is set, the reply is an error */
	private static final int NBD_REP_ERR_UNSUP = (1 | NBD_REP_FLAG_ERROR); /** Client requested an option not understood by this version of the server */
	private static final int NBD_REP_ERR_POLICY = (2 | NBD_REP_FLAG_ERROR); /** Client requested an option not allowed by server configuration. (e.g., the option was disabled) */
	private static final int NBD_REP_ERR_INVALID = (3 | NBD_REP_FLAG_ERROR); /** Client issued an invalid request */
	private static final int NBD_REP_ERR_PLATFORM = (4 | NBD_REP_FLAG_ERROR);

	private CountKeyDataDasd ckd;

	public Server(String dasdFileName) throws IOException {
		ckd = new CountKeyDataDasd(dasdFileName);
	}

	/** Client request list of supported exports (not followed by data) 
	 * @throws IOException */

	public static void main(String... args) throws IOException {
		new Server(args[0]).run();
	}

	/**
	 * nbd server runs in IANA port 10809
	 */
	public void run() {
		try {
//			Channel ch = System.inheritedChannel();
//			ServerSocketChannel ssc = (ServerSocketChannel) ch;
			ServerSocketChannel ssc = ServerSocketChannel.open().bind(new InetSocketAddress(InetAddress.getByName(null), PORT));

				SocketChannel sc = ssc.accept();
				String exportName = doHandshake(sc);

				System.out.println(exportName);

				/* transmission mode */
				int len;

				while(true) {
					ByteBuffer bb = readData(sc, 28);

					if(bb.getInt() != 0x25609513) {
						// FIXME: what to do? reply anyway?
						continue;
					}

					short commandFlags = bb.getShort();
					short type = bb.getShort();
					long handle = bb.getLong();
					long offset = bb.getLong(); //FIXME: unsigned!
					int length = bb.getInt(); //FIXME: unsigned!

					switch(type) {
					case NBD_CMD_READ:
					{
						ByteBuffer data = ckd.readTrackByOffset(exportName, offset, length);
						sendSimpleReply(sc, 0, handle, data);
						break;
					}
//					case NBD_CMD_WRITE:
//					{
//						ByteBuffer data = readData(sc, length);
////						ckd.write();
//						break;
//					}
					case NBD_CMD_DISC:
					{
						sc.close();
						break;
					}
//					case NBD_CMD_FLUSH:
//					case NBD_CMD_TRIM:

					default:
						sendSimpleReply(sc, NBD_REP_ERR_INVALID, handle, bb);
				}
			}
		} catch (IOException e) {
			Logger.getLogger(Server.class.getName()).log(Level.SEVERE, "failed!", e);
		}
	}

	private void sendSimpleReply(SocketChannel sc, int error, long handle, ByteBuffer data) throws IOException {
		ByteBuffer bbr = ByteBuffer.allocate(16);

		bbr.putInt(0x67446698);
		bbr.putInt(error);
		bbr.putLong(handle);
		sc.write(bbr);
		writeData(sc, bbr);
		if(data != null) {
			writeData(sc, data);
		}
	}

	/**
	 * do the handshake with the client
	 * @param s
	 * @return 
	 * @throws IOException
	 */
	private String doHandshake(SocketChannel s) throws IOException {

		/* initiate handshake */
		ByteBuffer bb = ByteBuffer.allocate(20);
		bb.putLong(0x4e42444d41474943l);
		bb.putLong(0x49484156454F5054l);

		/* "Global flags" */
		short flags = NBD_FLAG_FIXED_NEWSTYLE & NBD_FLAG_NO_ZEROES;
		bb.putShort(flags);
		writeData(s, bb);

		ByteBuffer bbin = readData(s, 4);
		int clientFlags = bbin.getInt();
		if((clientFlags & NBD_FLAG_FIXED_NEWSTYLE) == 0) throw new IllegalArgumentException();

		/* "option haggling" */
		while(true) {
			bbin = readData(s, 16);

			if(bbin.getLong() != 0x49484156454F5054l) throw new IllegalArgumentException();

			int option = bbin.getInt();
			int optionLen = bbin.getInt(); //FIXME: unsigned int!

			switch(option) {
			case NBD_OPT_EXPORT_NAME:
				bbin = readData(s, optionLen);

				byte[] nba = new byte[bbin.remaining()];
				bbin.get(nba);
				String exportName = new String(nba, "UTF-8");

				long exportSize = ckd.getPartitionSize(exportName);

				if(exportSize < 0) {
					//FIXME: send error response, partition doesn't exists!
					throw new IllegalArgumentException();
				}

				/* build response */
				bb.clear();
				bb.putLong(exportSize);
				short transmissionFlags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_READ_ONLY;
				bb.putShort(transmissionFlags);
				writeData(s, bb);

				if((clientFlags & NBD_FLAG_NO_ZEROES) == 0) {
					ByteBuffer bbz = ByteBuffer.allocate(124);
					bbz.position(124);
					writeData(s, bbz);
				}

				return exportName;

			default:
				sendOptionHagglingReply(s, option, NBD_REP_ERR_UNSUP, null);
			}
		}
	}

	private static ByteBuffer readData(SocketChannel s, int len) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(len);
		while(bb.position() != bb.limit()) {
			int l = s.read(bb);
		}
		bb.flip();
		return bb;
	}

	private static void writeData(SocketChannel s, ByteBuffer bb) throws IOException {
		bb.flip();
		int len = s.write(bb);
		if(len != bb.limit()) {
			throw new IllegalArgumentException("write short!");
		}
	}

	private void sendOptionHagglingReply(SocketChannel s, int option, int error, ByteBuffer data) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(18);

		bb.putLong(0x3e889045565a9l);
		bb.putInt(option);
		bb.putInt(error);
		int len = 0;
		if(data != null)
			len = data.remaining();
		bb.putInt(len);
		bb.flip();
		len = s.write(bb);
		if(data != null)
			len = s.write(data);
	}
}
