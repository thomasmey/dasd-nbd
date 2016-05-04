package de.m3y3r.dasdnbd;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/*
 * Count Key Data DASD reader/writer in null format 2 (linux)
 */
public class CountKeyDataDasd {

	private ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;
	private FileChannel channel;
	private Map<String, Object> deviceHeader;
	private Map<String, Number> compressedDeviceHeader;
	private MappedByteBuffer level1Table;
	private Charset ebcdicCharset;
	private Map<String, Map<String, Number>> partitions;

	public CountKeyDataDasd(String dasdFileName) throws IOException {
		ebcdicCharset = Charset.forName("IBM-037");
		partitions = new HashMap<>();

		openCkdImage(dasdFileName);
	}

	private void openCkdImage(String fname) throws IOException {
		File ckdFile = new File(fname);
		channel = FileChannel.open(ckdFile.toPath());

		deviceHeader = readDeviceHeader();
		compressedDeviceHeader = readCompressedDeviceHeader();
		level1Table = mapLevel1Table((int) compressedDeviceHeader.get("sizeLevel1Table"));

		ByteBuffer nullTrack = readTrack(0, 0);

		// read VOL1 - FIXME: where is this struct VOL1 described?
		ByteBuffer[] vol1 = readRecord(nullTrack, 3);
		vol1[1].order(ByteOrder.BIG_ENDIAN);
		vol1[1].position(11);
		int vtocCylinder = u16ToInt(vol1[1].getShort());
		int vtocHead = u16ToInt(vol1[1].getShort());
		int vtocRecordNo = vol1[1].get();

		ByteBuffer vtocTrack = readTrack(vtocCylinder, vtocHead);
		for(int i = vtocRecordNo, n = 255; i < n; i++) {
			ByteBuffer[] vtocRecord = readRecord(vtocTrack, i);
			if(vtocRecord == null) break;

			// add all partitions
			processDatasetControlBlock(vtocRecord);
		}
	}

	private void processDatasetControlBlock(ByteBuffer[] vtocRecord) {
		byte fmtId = vtocRecord[1].get();
		switch(fmtId) {
		/* we are actually only interessted in DSCB1 entries */
		case (byte) 0xf1: 
			Map<String, Object> f1 = readFormat1(vtocRecord[1]);
			byte[] dsExt1 = (byte[]) f1.get("dsExt1");
			Map<String, Number> dsExt1s = readDatasetExtent(dsExt1);
			dsExt1s.put("sectorSize", (int) f1.get("blockLength"));
			String datasetName = new String(vtocRecord[0].array(), ebcdicCharset).trim();
			partitions.put(datasetName, dsExt1s);
			break;
		default:
		}
	}

	private Map<String, Number> readDatasetExtent(byte[] dsExt) {
		ByteBuffer bb = ByteBuffer.wrap(dsExt);

		Map<String, Number> s = new HashMap<>();
		s.put("type", bb.get());
		s.put("sequenceNumber", bb.get());
		s.put("beginCylinder", u16ToInt(bb.getShort()));
		s.put("beginTrack", u16ToInt(bb.getShort()));
		s.put("endCylinder", u16ToInt(bb.getShort()));
		s.put("endTrack", u16ToInt(bb.getShort()));
		return s;
	}

	private Map<String, Object> readFormat1(ByteBuffer bb) {

		Map<String, Object> s = new HashMap<>();
		{
			byte[] ba = new byte[6];
			bb.get(ba);
			s.put("volumeSerialNumber", ba);
		}
		s.put("volumeSequenceNumber", u16ToInt(bb.getShort()));
		{
			byte[] ba = new byte[3];
			bb.get(ba);
			s.put("datasetCreationDate", ba);
		}
		{
			byte[] ba = new byte[3];
			bb.get(ba);
			s.put("datasetExpiryDate", ba);
		}
		s.put("noExtents", bb.get());
		s.put("noBytesLastDirectoryBlock", bb.get());
		bb.get(); // resv1
		{
			byte[] ba = new byte[13];
			bb.get(ba);
			s.put("systemCode", ba);
		}
		bb.get(new byte[7]); // resv2
		{
			byte[] ba = new byte[2];
			bb.get(ba);
			s.put("datasetOrganization", ba);
		}
		s.put("recordFormat", bb.get());
		s.put("optionCodes", bb.get());
		s.put("blockLength", u16ToInt(bb.getShort()));
		s.put("logicalRecordLength", u16ToInt(bb.getShort()));
		s.put("keyLength", bb.get());
		s.put("relativeKeyPosition", u16ToInt(bb.getShort()));
		s.put("datasetIndicators", bb.get());
		{
			byte[] ba = new byte[4];
			bb.get(ba);
			s.put("secondaryAllocation", ba);
		}
		{
			byte[] ba = new byte[3];
			bb.get(ba);
			s.put("lastUsedTIR", ba); //?? WAT IS THIS?
		}
		s.put("bytesUnusedLastTrack", u16ToInt(bb.getShort()));
		bb.get(new byte[2]); // resv3
		{
			byte[] ba = new byte[10];
			bb.get(ba);
			s.put("dsExt1", ba);
		}
		{
			byte[] ba = new byte[10];
			bb.get(ba);
			s.put("dsExt2", ba);
		}
		{
			byte[] ba = new byte[10];
			bb.get(ba);
			s.put("dsExt3", ba);
		}
		{
			byte[] ba = new byte[5];
			bb.get(ba);
			s.put("cchhrF2F3DSCB", ba);
		}
		return s;
	}

	/** 
	 * 
	 * @param trackData trackdata without track header
	 * @param recordNo which record to read
	 */
	ByteBuffer[] readRecord(ByteBuffer trackData, int recordNo) {
		while(true) {
			Map<String, Number> recordHeader = readRecordHeader(trackData);
			if(recordHeader == null)
				return null;

			int kl = recordHeader.get("keyLength").intValue();
			int dl = recordHeader.get("dataLength").intValue();
			int rn = recordHeader.get("recordNo").intValue();

			if(rn == recordNo) {
				int l = trackData.limit();

				trackData.limit(trackData.position() + kl);
				ByteBuffer key = ByteBuffer.allocate(kl);
				key.put(trackData);
				key.flip();
				trackData.limit(trackData.position() + dl);
				ByteBuffer data = ByteBuffer.allocate(dl);
				data.put(trackData);
				data.flip();

				trackData.limit(l);

				return new ByteBuffer[] {key, data};
			}
			trackData.position(trackData.position() + kl + dl);
		}
	}

	private Map<String, Number> readRecordHeader(ByteBuffer trackData) {

		// check end of track marker
		{
			int cp = trackData.position();
			if(trackData.asLongBuffer().get() == -1)
				return null;
			trackData.position(cp);
		}

		trackData.order(ByteOrder.BIG_ENDIAN);
		Map<String, Number> s = new HashMap<>();
		s.put("cylinder", u16ToInt(trackData.getShort()));
		s.put("head", u16ToInt(trackData.getShort()));
		s.put("recordNo", trackData.get());
		s.put("keyLength", trackData.get());
		s.put("dataLength", u16ToInt(trackData.getShort()));
		return s;
	}

	private MappedByteBuffer mapLevel1Table(int level1TableSize) throws IOException {
		MappedByteBuffer level1Table = channel.map(MapMode.READ_ONLY, channel.position(), level1TableSize * Integer.BYTES);
		level1Table.order(byteOrder);
		return level1Table;
	}

	public static long u32ToLong(int v) {
		return (long)v & 0xff_ff_ff_ffl;
	}

	public static int u16ToInt(short v) {
		return v & 0xff_ff;
	}

	private Map<String, Object> readDeviceHeader() throws IOException {
		ByteBuffer bb = read(512);

		Map<String, Object> s = new HashMap<>();
		byte[] deviceId = new byte[8];
		bb.get(deviceId);
		s.put("deviceId", new String(deviceId, "US-ASCII"));
		s.put("noHeads", bb.getInt());
		s.put("trackSize", bb.getInt());
		s.put("deviceType", bb.get());
		s.put("fileSequenceNo", bb.get());
		s.put("highesCylinderOrNull", bb.getShort());
		return s;
	}

	private Map<String, Number> readCompressedDeviceHeader() throws IOException {
		ByteBuffer bb = read(512);

		Map<String, Number> s = new HashMap<>();
		s.put("v", bb.get());
		s.put("r", bb.get());
		s.put("m", bb.get());
		s.put("options", bb.get());

		/* option flags */
		if((s.get("options").byteValue() & 2) == 1)
			byteOrder = ByteOrder.BIG_ENDIAN;
		bb.order(byteOrder);

		s.put("sizeLevel1Table", bb.getInt());
		s.put("sizeLevel2Table", bb.getInt());
		s.put("fileSize", bb.getInt());
		s.put("fileUsed", bb.getInt());
		s.put("positionToFreeSpace", bb.getInt());
		s.put("totalFreeSpace", bb.getInt());
		s.put("numberFreeSpaces", bb.getInt());
		s.put("largestFreeSpace", bb.getInt());
		s.put("ImbeddedFreeSpace", bb.getInt());
		s.put("noCylindersOnDevice", bb.getInt());
		s.put("nullTrackFormat", bb.get());
		s.put("compressAlgorithm", bb.get());
		s.put("compressParameter", bb.getShort());
		return s;
	}

	/* Track is:
	 *  - Track header 1
	 *    - Record header (1-n)
	 *    - key 0-1
	 *    - data 0-1
	 *  - end of track marker
	 */
	private ByteBuffer read(int noBytes) throws IOException {
		return read(noBytes, byteOrder);
	}

	private ByteBuffer read(int noBytes, ByteOrder byteOrder) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(noBytes);

		//FIXME: check read len vs. requestes len
		int len = channel.read(bb);
		bb.flip();
		bb.order(byteOrder);
		return bb;
	}


	private ByteBuffer readTrack(long track) throws IOException {

		long pos = readLevel1Entry(track);
		if(pos == 0) {
			/* empty L2 table, L2 table not yet used...! */
			return nullTrack(track, compressedDeviceHeader.get("nullTrackFormat").intValue());
		}

		channel.position(pos);

		Map<String, Number> l2Entry = readLevel2Entry(track);
		long posTrack = l2Entry.get("position").longValue();

		/* FIXME:
		 * posTrack == 0 - Unused track?! Not yet used?
		 * posTrack == 0xffffffff - Track is not in this file?!
		 */
		if(posTrack == 0) {
			/* length and size field are mis-used for "null track format information... */
			return nullTrack(track, l2Entry.get("length").intValue());
		}
		channel.position(posTrack);

		Map<String, Number> trackHeader = readTrackHeader();

		//FIXME: size or length?!
		ByteBuffer trackData = read(l2Entry.get("size").intValue());

		int optComp = trackHeader.get("options").intValue() & 0xf;

		switch(optComp) {
		case 0:
			return trackData;

		case 1:
			Inflater i = new Inflater();
			i.setInput(trackData.array());
			byte[] u = new byte[(int) Math.pow(2, 16)];
			try {
				int len = i.inflate(u);
				ByteBuffer o = ByteBuffer.wrap(u);
				o.limit(len);
				return o;
			} catch (DataFormatException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public ByteBuffer readTrack(long cylinder, int head) throws IOException {
		long trk = getTrackNo(cylinder, (short) head);
		return readTrack(trk);
	}

	private ByteBuffer nullTrack(long track, int nullTrackFormat) {
		ByteBuffer trackData = ByteBuffer.allocate((int)deviceHeader.get("trackSize"));

		//FIXME: what byte order?
//		trackData.order() ??
		long ch[] = cylHeadFromTrack(track);
		int cylinder = (int) ch[0];
		short head = (short) ch[1];

		writeRecord(trackData, (int) cylinder, head, (short) 0, null, ByteBuffer.allocate(8));

		switch(nullTrackFormat) {
		case 0:
			writeRecord(trackData, (int) cylinder, head, (short) 1, null, null);
			break;
		case 2:
			ByteBuffer blockSize = ByteBuffer.allocate(4096);
			for(short r = 1; r <= 12; r++) {
				blockSize.clear();
				writeRecord(trackData, (int) cylinder, head, r, null, blockSize);
			}
		}
		/* add end of track marker */
		for(int i = 0, n= 8; i < n; i++)
			trackData.put((byte) 0xff);

		trackData.flip();
		return trackData;
	}

	private long[] cylHeadFromTrack(long track) {
		int noHeadsPerCylinder = (int)deviceHeader.get("noHeads");
		long cyl = track / noHeadsPerCylinder;
		long head = track % noHeadsPerCylinder;
		return new long[] {cyl, head};
	}

	private void writeRecord(ByteBuffer trackData, int cylinder, int head, short recordNo, ByteBuffer key, ByteBuffer data) {
		int kl = (key != null) ? key.limit() : 0;
		int dl = (data != null) ? data.limit() : 0;

		writeRecordHeader(trackData, cylinder, head, recordNo, (short) kl, dl);
		if(kl > 0) trackData.put(key);
		if(dl > 0) trackData.put(data);
	}

	private void writeRecordHeader(ByteBuffer trackData, int cylinder, int head, short recordNo, short kl, int dl) {
		trackData.order(ByteOrder.BIG_ENDIAN);
		trackData.putShort((short) cylinder);
		trackData.putShort((short) head);
		trackData.put((byte) recordNo);
		trackData.put((byte) kl);
		trackData.putShort((short) dl);
	}

	private Map<String, Number> readTrackHeader() throws IOException {
		ByteBuffer trackHeader = read(5, ByteOrder.BIG_ENDIAN);
		Map<String, Number> s2 = new HashMap<>();
		s2.put("options", trackHeader.get());
		s2.put("cylinder", u16ToInt(trackHeader.getShort()));
		s2.put("head", u16ToInt(trackHeader.getShort()));
		return s2;
	}

	private Map<String, Number> readLevel2Entry(long trk) throws IOException {
		int l2entrySize = 8;
		ByteBuffer level2Entries = read(compressedDeviceHeader.get("sizeLevel2Table").intValue() * l2entrySize);

		int l2ent = (int) (trk % compressedDeviceHeader.get("sizeLevel2Table").intValue());
		level2Entries.position(l2ent * l2entrySize);

		Map<String, Number> s = new HashMap<>();
		s.put("position", u32ToLong(level2Entries.getInt()));
		s.put("length", u16ToInt(level2Entries.getShort()));
		s.put("size", u16ToInt(level2Entries.getShort()));
		return s;
	}

	/*FIXME: cylinder is long or int? */
	private long getTrackNo(long cylinder, short head) {
		return (cylinder * (int)deviceHeader.get("noHeads")) + head;
	}

	private long readLevel1Entry(long trk) {
		int l1ent = (int) (trk / compressedDeviceHeader.get("sizeLevel2Table").intValue());
		long pos = u32ToLong(level1Table.asIntBuffer().get(l1ent));
		return pos;
	}

	public long getPartitionSize(String exportName) throws IOException {
		Map<String, Number> s = partitions.get(exportName);
		if(s == null) return -1;

		long beginCyl = s.get("beginCylinder").longValue(),
			endCyl = s.get("endCylinder").longValue(),
			beginHead = s.get("beginTrack").intValue(),
			endHead = s.get("endTrack").longValue();

		long endTrack = getTrackNo(endCyl, (short) endHead);
		long beginTrack = getTrackNo(beginCyl, (short) beginHead);

		long totalTracks = endTrack - beginTrack + 1; //FIXME +1 correct?

		long size = totalTracks * 12 * 4096;
		return size;
	}

	public ByteBuffer readTrackByOffset(String exportName, long offset, int length) throws IOException {
		Map<String, Number> s = partitions.get(exportName);
		if(s == null) throw new IllegalArgumentException();

//		System.out.println("read data with offset=" + offset + " length=" + length);
		long beginCyl = s.get("beginCylinder").longValue(),
			beginHead = s.get("beginTrack").intValue();

		long beginTrack = getTrackNo(beginCyl, (short) beginHead);
		ByteBuffer dataTotal = ByteBuffer.allocate(length);

		int sectorSize = 4096;
		int sectorsPerTrack = 12;

		long trackRel = offset / (sectorSize * sectorsPerTrack);
		long offsetRel = offset % (sectorSize * sectorsPerTrack);
		long trackTotal = beginTrack + trackRel;

		ByteBuffer trackData = readTrack(trackTotal);
		int sector = (int) (offsetRel / sectorSize);
		int sectorRel = (int) (offsetRel % sectorSize);

		long[] ch = cylHeadFromTrack(trackTotal);
//		System.out.println("cyl=" + ch[0] + " head=" + ch[1]);
//		System.out.println("trk=" + trackTotal + " offset=" + offset + " relOffset=" + offsetRel + " sector=" + sector);
		while(length > 0) {
			ByteBuffer[] keyData = readRecord(trackData, ++sector);

			if(sectorRel > 0) {
				keyData[1].position(sectorRel);
				sectorRel = 0;
			}
			// length from nbd client doesn't always seems to be a multiple of sector size!
			if(dataTotal.remaining() < keyData[1].remaining()) {
				keyData[1].limit(keyData[1].position() + dataTotal.remaining());
			}
			dataTotal.put(keyData[1]);
			length -= keyData[1].limit();
			offset += keyData[1].limit();
			if(sector >= sectorsPerTrack) { sector = 0; trackData = readTrack(++trackTotal);}
		};
		return dataTotal;
	}
}
