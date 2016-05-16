package de.m3y3r.dasdnbd;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/*
 * Count Key Data DASD reader/writer in null format 2 (linux)
 */
public class CountKeyDataDasd implements Closeable {

	private static final long SECTORS_PER_TRACK = 12;
	private static final long SECTOR_SIZE = 4096;
	private static final int FREE_SPACE_BLOCK_LENGTH = 8;
	private static final int L2_ENTRY_SIZE = 8;

	private ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;
	private FileChannel channel;
	private Map<String, Object> deviceHeader;
	private Map<String, Number> compressedDeviceHeader;
	private MappedByteBuffer level1Table;
	private Charset ebcdicCharset;
	private Map<String, Map<String, Number>> partitions;
	private MappedByteBuffer freeSpaceMap;

	public CountKeyDataDasd(String dasdFileName) throws IOException {
		ebcdicCharset = Charset.forName("IBM-037");
		partitions = new HashMap<>();

		openCkdImage(dasdFileName);
	}

	private void openCkdImage(String fname) throws IOException {
		File ckdFile = new File(fname);
		channel = FileChannel.open(ckdFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);

		deviceHeader = readDeviceHeader();
		compressedDeviceHeader = readCompressedDeviceHeader();
		level1Table = mapLevel1Table((int) compressedDeviceHeader.get("sizeLevel1Table"));

		ByteBuffer nullTrack = readTrack(0, 0);

		// read VOL1 - FIXME: where is this struct VOL1 described?
		ByteBuffer[] vol1 = readRecord(nullTrack, 3);
		vol1[1].order(ByteOrder.BIG_ENDIAN);
		vol1[1].position(11);
		int vtocCylinder = ByteUtil.u16ToInt(vol1[1].getShort());
		int vtocHead = ByteUtil.u16ToInt(vol1[1].getShort());
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
		/* we are actually only interested in DSCB1 entries */
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
		s.put("beginCylinder", ByteUtil.u16ToInt(bb.getShort()));
		s.put("beginTrack", ByteUtil.u16ToInt(bb.getShort()));
		s.put("endCylinder", ByteUtil.u16ToInt(bb.getShort()));
		s.put("endTrack", ByteUtil.u16ToInt(bb.getShort()));
		return s;
	}

	private Map<String, Object> readFormat1(ByteBuffer bb) {

		Map<String, Object> s = new HashMap<>();
		{
			byte[] ba = new byte[6];
			bb.get(ba);
			s.put("volumeSerialNumber", ba);
		}
		s.put("volumeSequenceNumber", ByteUtil.u16ToInt(bb.getShort()));
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
		s.put("blockLength", ByteUtil.u16ToInt(bb.getShort()));
		s.put("logicalRecordLength", ByteUtil.u16ToInt(bb.getShort()));
		s.put("keyLength", bb.get());
		s.put("relativeKeyPosition", ByteUtil.u16ToInt(bb.getShort()));
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
		s.put("bytesUnusedLastTrack", ByteUtil.u16ToInt(bb.getShort()));
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
		return (ByteBuffer[]) processRecord(trackData, recordNo, (rh, t) -> {
			int kl = rh.get("keyLength").intValue();
			int dl = rh.get("dataLength").intValue();

			int l = t.limit();

			t.limit(t.position() + kl);
			ByteBuffer key = ByteBuffer.allocate(kl);
			key.put(t);
			key.rewind();

			t.limit(t.position() + dl);
			ByteBuffer data = ByteBuffer.allocate(dl);
			data.put(t);
			data.rewind();

			t.limit(l);

			return new ByteBuffer[] {key, data};
		});
	}

	private void updateRecord(ByteBuffer trackData, int recordNo, ByteBuffer[] keyData) {
		/*FIXME: what happens when the key and data length are shorter then the
		 * existing ones? I think this will totally fuck up the track data
		 * The remaining data needs to be moved to the correct location
		 */
		
		processRecord(trackData, recordNo, (rh, t) -> {
			int kl = rh.get("keyLength").intValue();
			int dl = rh.get("dataLength").intValue();

			assert kl == keyData[0].remaining();
			assert dl == keyData[1].remaining();
			keyData[0].limit(kl);
			keyData[1].limit(dl);
			trackData.put(keyData[0]);
			trackData.put(keyData[1]);
			return null;
		});
	}

	private Object processRecord(ByteBuffer trackData, int recordNo, BiFunction<Map<String, Number>, ByteBuffer,Object> processor) {
		while(true) {
			Map<String, Number> recordHeader = readRecordHeader(trackData);
			if(recordHeader == null)
				return null;

			int kl = recordHeader.get("keyLength").intValue();
			int dl = recordHeader.get("dataLength").intValue();
			int rn = recordHeader.get("recordNo").intValue();

			if(rn == recordNo) {
				return processor.apply(recordHeader, trackData);
			}
			trackData.position(trackData.position() + kl + dl);
		}
	}

	private Map<String, Number> readRecordHeader(ByteBuffer trackData) {

		// check end of track marker
		{
			trackData.mark();
			if(trackData.asLongBuffer().get() == -1)
				return null;
			trackData.reset();
		}

		trackData.order(ByteOrder.BIG_ENDIAN);
		Map<String, Number> s = new HashMap<>();
		s.put("cylinder", ByteUtil.u16ToInt(trackData.getShort()));
		s.put("head", ByteUtil.u16ToInt(trackData.getShort()));
		s.put("recordNo", trackData.get());
		s.put("keyLength", trackData.get());
		s.put("dataLength", ByteUtil.u16ToInt(trackData.getShort()));
		return s;
	}

	private MappedByteBuffer mapLevel1Table(int level1TableSize) throws IOException {
		MappedByteBuffer level1Table = channel.map(MapMode.READ_WRITE, channel.position(), level1TableSize * Integer.BYTES);
		level1Table.order(byteOrder);
		return level1Table;
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
		s.put("fileSize", ByteUtil.u32ToLong(bb.getInt()));
		s.put("fileUsed", ByteUtil.u32ToLong(bb.getInt()));
		s.put("positionToFreeSpace", ByteUtil.u32ToLong(bb.getInt()));
		s.put("totalFreeSpace", ByteUtil.u32ToLong(bb.getInt()));
		s.put("largestFreeSpace", ByteUtil.u32ToLong(bb.getInt()));
		s.put("numberFreeSpaces", bb.getInt());
		s.put("imbeddedFreeSpace", ByteUtil.u32ToLong(bb.getInt())); //FIXME: what is this field?!
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

	/**
	 * reads a track without track header
	 * @param track
	 * @return
	 * @throws IOException
	 */
	private ByteBuffer readTrack(long track) throws IOException {

		long l2BasePos = readLevel1Entry(track);
		if(l2BasePos == 0) {
			/* empty L2 table, L2 table not yet used...! */
			return createNullTrack(track, compressedDeviceHeader.get("nullTrackFormat").intValue());
		}

		Map<String, Number> l2Entry = readLevel2Entry(l2BasePos, track);
		long posTrack = l2Entry.get("position").longValue();

		/* FIXME:
		 * posTrack == 0 - Unused track?! Not yet used?
		 * posTrack == 0xffffffff - Track is not in this file?!
		 */
		if(posTrack == 0) {
			/* length and size field are mis-used for "null track format information... */
			return createNullTrack(track, l2Entry.get("length").intValue());
		}
		channel.position(posTrack);

		Map<String, Number> trackHeader = readTrackHeader();

		ByteBuffer trackData = read(l2Entry.get("length").intValue());

		int optComp = trackHeader.get("options").intValue() & 0xf;
		switch(optComp) {
		case 0:
			return trackData;

		case 1:
			Inflater i = new Inflater();
			i.setInput(trackData.array(), 0, trackData.limit());
			ByteBuffer ud = ByteBuffer.allocate((int) Math.pow(2, 16));
			byte[] buffer = new byte[1024];
			try {
				while(!i.finished()) {
					int len = i.inflate(buffer);
					assert len >= 0 : "invalid compressed data";
					ud.put(buffer, 0, len);
				}
				ud.flip();
				return ud;
			} catch (DataFormatException e) {
				e.printStackTrace();
			}
			break;
		default:
			throw new IllegalArgumentException("Unknonw compression method " + optComp + " for trackNo "+ track); 
		}

		return null;
	}

	public ByteBuffer readTrack(long cylinder, int head) throws IOException {
		long trk = getTrackNo(cylinder, (short) head);
		return readTrack(trk);
	}

	private ByteBuffer createNullTrack(long track, int nullTrackFormat) {
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
			ByteBuffer blockSize = ByteBuffer.allocate((int) SECTOR_SIZE);
			for(short r = 1; r <= SECTORS_PER_TRACK; r++) {
				blockSize.clear();
				writeRecord(trackData, (int) cylinder, head, r, null, blockSize);
			}
		}
		/* add end of track marker */
		for(int i = 0, n = 8; i < n; i++)
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
		Map<String, Number> s = new HashMap<>();
		s.put("options", trackHeader.get());
		s.put("cylinder", ByteUtil.u16ToInt(trackHeader.getShort()));
		s.put("head", ByteUtil.u16ToInt(trackHeader.getShort()));
		return s;
	}

	private ByteBuffer createTrackHeader(long trackNo, byte compAlg) {

		char[] ch = getCylinderHeader(trackNo);

		ByteBuffer trackHeader = ByteBuffer.allocate(5).order(ByteOrder.BIG_ENDIAN);
		trackHeader.put(compAlg);
		trackHeader.putShort((short) ch[0]);
		trackHeader.putShort((short) ch[1]);
		trackHeader.rewind();
		return trackHeader;
	}

	/*FIXME: File position needs to be set to the correct position before calling this method!!! */
	private Map<String, Number> readLevel2Entry(long l2BasePos, long trk) throws IOException {
		channel.position(l2BasePos);
		ByteBuffer level2Entries = read(compressedDeviceHeader.get("sizeLevel2Table").intValue() * L2_ENTRY_SIZE);

		int l2ent = (int) (trk % compressedDeviceHeader.get("sizeLevel2Table").intValue());
		level2Entries.position(l2ent * L2_ENTRY_SIZE);

		Map<String, Number> s = new HashMap<>();
		long pos = ByteUtil.u32ToLong(level2Entries.getInt());
		s.put("position", pos);
		s.put("length", ByteUtil.u16ToInt(level2Entries.getShort()));
		s.put("size", ByteUtil.u16ToInt(level2Entries.getShort()));
		return s;
	}

	private void writeLevel2Entry(long level2EntryBasePos, long trackNo, Map<String, Number> level2Entry) throws IOException {

		ByteBuffer l2Entry = ByteBuffer.allocate(L2_ENTRY_SIZE).order(byteOrder);
		l2Entry.putInt(level2Entry.get("position").intValue());
		l2Entry.putShort(level2Entry.get("length").shortValue());
		l2Entry.putShort(level2Entry.get("size").shortValue());
		l2Entry.rewind();

		int l2ent = (int) (trackNo % compressedDeviceHeader.get("sizeLevel2Table").intValue());
		this.channel.position(level2EntryBasePos + (l2ent * L2_ENTRY_SIZE));
		this.channel.write(l2Entry);
	}

	/*FIXME: cylinder is long or int? */
	private long getTrackNo(long cylinder, short head) {
		return (cylinder * (int)deviceHeader.get("noHeads")) + head;
	}

	//cylinder, head are actually 16 bit unsigned, may use char type here!
	private char[] getCylinderHeader(long trackNo) {

		int nh = (int)deviceHeader.get("noHeads");
		return new char[] { (char) (trackNo / nh), (char) (trackNo % nh)};
	}

	private long readLevel1Entry(long trk) {
		int l1ent = (int) (trk / compressedDeviceHeader.get("sizeLevel2Table").intValue());
		long pos = ByteUtil.u32ToLong(level1Table.asIntBuffer().get(l1ent));
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

		long size = totalTracks * SECTORS_PER_TRACK * SECTOR_SIZE;
		return size;
	}

	public ByteBuffer readDataByOffset(String exportName, long offset, int length) throws IOException {
		Map<String, Number> s = partitions.get(exportName);
		if(s == null) throw new IllegalArgumentException();

		if(offset < 0 || offset >= getPartitionSize(exportName)) {
			throw new IllegalArgumentException("Illegal offset " + offset + " should between 0 and " + getPartitionSize(exportName));
		}

		long beginCyl = s.get("beginCylinder").longValue(),
			beginHead = s.get("beginTrack").intValue();

		long beginTrack = getTrackNo(beginCyl, (short) beginHead);
		ByteBuffer dataTotal = ByteBuffer.allocate(length);

		long trackRel = offset / (SECTOR_SIZE * SECTORS_PER_TRACK);
		long offsetRel = offset % (SECTOR_SIZE * SECTORS_PER_TRACK);
		long trackTotal = beginTrack + trackRel;

		ByteBuffer trackData = readTrack(trackTotal);
		int sector = (int) (offsetRel / SECTOR_SIZE);
		int sectorRel = (int) (offsetRel % SECTOR_SIZE);

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
			if(sector >= SECTORS_PER_TRACK) { sector = 0; trackData = readTrack(++trackTotal);}
		};
		return dataTotal;
	}

	public void writeDataByOffset(String exportName, long offset, ByteBuffer data) throws IOException {
		Map<String, Number> s = partitions.get(exportName);
		if(s == null) throw new IllegalArgumentException();

		if(offset < 0 || offset >= getPartitionSize(exportName)) {
			throw new IllegalArgumentException("Illegal offset " + offset + " should between 0 and " + getPartitionSize(exportName));
		}

		long beginCyl = s.get("beginCylinder").longValue(),
			beginHead = s.get("beginTrack").intValue();

		long beginTrack = getTrackNo(beginCyl, (short) beginHead);

		long trackRel = offset / (SECTOR_SIZE * SECTORS_PER_TRACK);
		long offsetRel = offset % (SECTOR_SIZE * SECTORS_PER_TRACK);
		long trackTotal = beginTrack + trackRel;

		ByteBuffer trackData = readTrack(trackTotal);
		int sector = (int) (offsetRel / SECTOR_SIZE);
		int sectorRel = (int) (offsetRel % SECTOR_SIZE);

		while(data.hasRemaining()) {
			trackData.mark();
			ByteBuffer[] keyData = readRecord(trackData, ++sector);

			if(sectorRel > 0) {
				keyData[1].position(sectorRel);
				sectorRel = 0;
			}

			while(keyData[1].hasRemaining() && data.hasRemaining()) {
				keyData[1].put(data.get());
			}
			keyData[1].rewind();

			trackData.reset();
			updateRecord(trackData, sector, keyData);

			if(sector >= SECTORS_PER_TRACK) {
				writeTrack(trackTotal, trackData);
				sector = 0;
				trackData = readTrack(++trackTotal);
			}
		};
		writeTrack(trackTotal, trackData);
	}

	private void writeTrack(long trackNo, ByteBuffer trackData) throws IOException {

		assert trackData != null;

		long level2EntryBasePos = readLevel1Entry(trackNo);
		if(level2EntryBasePos == 0) { // unused level1 table entry
			ByteBuffer level2Table = createLevel2Table();
			level2EntryBasePos = allocateFreeSpace(level2Table.limit());
			writeLevel2Table(level2EntryBasePos, level2Table);
			writeLevel1Entry(trackNo, level2EntryBasePos);
		} else if(level2EntryBasePos == -1) {
			/* table is in another file */
			throw new IllegalArgumentException();
		}

		Map<String, Number> level2Entry = readLevel2Entry(level2EntryBasePos, trackNo);
		byte compAlg = compressedDeviceHeader.get("compressAlgorithm").byteValue();
		switch(compAlg) {
			case 1:
			// compress trackData with libz
			ByteBuffer comp = ByteBuffer.allocate((int) Math.pow(2, 16));

			Deflater d = new Deflater();
			byte[] buffer = new byte[1024];
			d.setInput(trackData.array(), 0, trackData.limit());
			d.finish();

			while(!d.finished()) {
				int len = d.deflate(buffer);
				comp.put(buffer, 0 , len);
			}
			comp.flip();
			trackData = comp;
			break;

			default:
				throw new IllegalArgumentException();
		}

		trackData.rewind();

		ByteBuffer trackHeader = createTrackHeader(trackNo, compAlg);

		/* Track size  (size >= len) */
		long oldTrackPos = level2Entry.get("position").longValue();
		int oldTrackLen = level2Entry.get("length").intValue();
		int oldTrackSize = level2Entry.get("size").intValue();

		assert oldTrackSize >= oldTrackLen: "size >= len failed for track: " + trackNo;

		int newTrackLen =  trackHeader.limit() + trackData.limit();
		long newTrackPos = oldTrackPos;
		long newTrackSize = oldTrackSize;

		// check for empty level2 entry
		if(oldTrackPos == 0) {
			newTrackPos = allocateFreeSpace(newTrackLen);
			newTrackSize = newTrackLen;
		}

		// check for in-place update
		if(newTrackLen > newTrackSize) {
			newTrackPos = allocateFreeSpace(newTrackLen);
			newTrackSize = newTrackLen;
			deallocateFreeSpace(oldTrackPos, oldTrackSize);
		}

		// write track header and data
		this.channel.position(newTrackPos);
		this.channel.write(trackHeader);
		this.channel.write(trackData);

		// update level2 entry
		level2Entry.put("position", newTrackPos);
		level2Entry.put("length", newTrackLen);
		level2Entry.put("size", newTrackSize);
		writeLevel2Entry(level2EntryBasePos, trackNo, level2Entry);
	}

	private void writeLevel1Entry(long trk, long level2EntryBasePos) {
		int l1ent = (int) (trk / compressedDeviceHeader.get("sizeLevel2Table").intValue());
		level1Table.asIntBuffer().put(l1ent, (int)level2EntryBasePos);
	}

	private void writeLevel2Table(long level2TableBasePos, ByteBuffer level2Table) throws IOException {
		this.channel.position(level2TableBasePos);
		this.channel.write(level2Table);
	}

	private void deallocateFreeSpace(long freeSpacePos, int freeSpaceSize) throws IOException {
		// add a new free space entry in the free space map increase no of free spaces in header field

		//FIXME: check max free space map length!!
		int noFreeSpaces = compressedDeviceHeader.get("numberFreeSpaces").intValue();
		noFreeSpaces++;

		// first block contains FREE_BLK!
		freeSpaceMap.position(FREE_SPACE_BLOCK_LENGTH + (noFreeSpaces * FREE_SPACE_BLOCK_LENGTH));
		compressedDeviceHeader.put("numberFreeSpaces", noFreeSpaces);
		freeSpaceMap.mark();
		updateFreeSpaceBlock(freeSpaceMap, freeSpacePos, freeSpaceSize);

		long totalFreeSpace = compressedDeviceHeader.get("totalFreeSpace").longValue();
		compressedDeviceHeader.put("totalFreeSpace", totalFreeSpace + freeSpaceSize);
		long fileUsed = compressedDeviceHeader.get("fileUsed").longValue();
		compressedDeviceHeader.put("fileUsed", fileUsed - freeSpaceSize);

		if(compressedDeviceHeader.get("largestFreeSpace").longValue() < freeSpaceSize) {
			compressedDeviceHeader.put("largestFreeSpace", (long) freeSpaceSize);
		}
	}

	private long allocateFreeSpace(int len) throws IOException {

		long freeSpacePos = -1;

		long lfs = compressedDeviceHeader.get("largestFreeSpace").longValue();
		if(len > lfs) {
			// too big to fit in a free space slot, append to end of file

			long fileSize = compressedDeviceHeader.get("fileSize").longValue();

			//FIXME: where to get the maximum file size from?
//			if(fileSize + len > deviceHeader.get("maxFileSize")) {
//				throw new IllegalArgumentException("dataset too small!");
//			}

			compressedDeviceHeader.put("fileSize", fileSize + len);
			long fileUsed = compressedDeviceHeader.get("fileUsed").longValue();
			compressedDeviceHeader.put("fileUsed", fileUsed + len);
			return fileSize;
		} else {
			// find free space slot
			long positionToFreeSpace = (long) compressedDeviceHeader.get("positionToFreeSpace");
			int noFreeSpaces = compressedDeviceHeader.get("numberFreeSpaces").intValue();

			if(freeSpaceMap == null) {
				/* test for old or new free block format */
				this.channel.position(positionToFreeSpace);
				ByteBuffer format = read(FREE_SPACE_BLOCK_LENGTH);
				if(!Arrays.equals("FREE_BLK".getBytes(), format.array())) {
					throw new IllegalAccessError("old free space block format not supported!");
				}

				// search free space block which describes the free space array itself!
				for(;;) {
					ByteBuffer fsb = read(FREE_SPACE_BLOCK_LENGTH);
					long[] freeBlockPosLen = readFreeSpaceBlock(fsb);
					if(positionToFreeSpace == freeBlockPosLen[0]) {
						freeSpaceMap = this.channel.map(MapMode.READ_WRITE, freeBlockPosLen[0], freeBlockPosLen[1]);
						freeSpaceMap.order(byteOrder);
						break;
					}
				}
			}

			freeSpaceMap.position(FREE_SPACE_BLOCK_LENGTH); // skip first entry "FREE_BLK)
			for(int i = 0; i < noFreeSpaces; i++) {

				freeSpaceMap.mark();
				long[] freeBlockPosLen = readFreeSpaceBlock(freeSpaceMap);

				//skip the entry, that describes this free space block array itself! 
				if(positionToFreeSpace == freeBlockPosLen[0])
					continue;

				if(len < freeBlockPosLen[1]) {
					/* data fits into current free block, assign position */
					long remainingFreeSpaceInBlock = (long) (freeBlockPosLen[1] - len);
					long newBlockPos = freeBlockPosLen[0] + len + 1;
					updateFreeSpaceBlock(freeSpaceMap, newBlockPos, remainingFreeSpaceInBlock);

					/* check for largest free space area */
					if(lfs == freeBlockPosLen[1]) {
						/* update device header field with new value */
						compressedDeviceHeader.put("largestFreeSpace", remainingFreeSpaceInBlock);
					}
					freeSpacePos = freeBlockPosLen[0];
					break;
				}
			}
		}

		// update header statistics
		if(freeSpacePos > 0) {
			long totalFreeSpace = compressedDeviceHeader.get("totalFreeSpace").longValue();
			compressedDeviceHeader.put("totalFreeSpace", totalFreeSpace - len);
			long fileUsed = compressedDeviceHeader.get("fileUsed").longValue();
			compressedDeviceHeader.put("fileUsed", fileUsed + len);
		}

		assert freeSpacePos >= 0;

		return freeSpacePos;
	}

	private void updateFreeSpaceBlock(MappedByteBuffer freeSpaceMap, long freeSpacePosition, long freeSpaceLength) throws IOException {

		freeSpaceMap.reset();
		freeSpaceMap.putInt((int) freeSpacePosition);
		freeSpaceMap.putInt((int) freeSpaceLength);
	}

	private long[] readFreeSpaceBlock(ByteBuffer freeSpaceBlock) throws IOException {
		long currentFreeBlockPosition = ByteUtil.u32ToLong(freeSpaceBlock.getInt());
		long currentFreeBlockLength = ByteUtil.u32ToLong(freeSpaceBlock.getInt());
		return new long[] {currentFreeBlockPosition, currentFreeBlockLength};
	}

	/* create empty l2 table */
	private ByteBuffer createLevel2Table() {
		int n = compressedDeviceHeader.get("sizeLevel2Table").intValue();
		short nullFormat = compressedDeviceHeader.get("nullTrackFormat").shortValue();

		ByteBuffer level2Entries = ByteBuffer.allocate(n * L2_ENTRY_SIZE).order(byteOrder);

		for(int i = 0; i < n; i++) {
			level2Entries.putInt(0);
			level2Entries.putShort(nullFormat);
			level2Entries.putShort(nullFormat);
		}
		level2Entries.rewind();
		return level2Entries;
	}

	@Override
	public void close() throws IOException {
		// write cache to data

		// FIXME: sync headers to disk!!
//		writeDeviceHeader(deviceHeader);
		writeCompressedDiskHeader(compressedDeviceHeader);
		channel.close();
	}

	private void writeCompressedDiskHeader(Map<String, Number> compressedDeviceHeader) throws IOException {

		ByteBuffer cdh = ByteBuffer.allocate(512).order(byteOrder);
		cdh.put(compressedDeviceHeader.get("v").byteValue());
		cdh.put(compressedDeviceHeader.get("r").byteValue());
		cdh.put(compressedDeviceHeader.get("m").byteValue());
		cdh.put(compressedDeviceHeader.get("options").byteValue());
		cdh.putInt(compressedDeviceHeader.get("sizeLevel1Table").intValue());
		cdh.putInt(compressedDeviceHeader.get("sizeLevel2Table").intValue());
		cdh.putInt(compressedDeviceHeader.get("fileSize").intValue());
		cdh.putInt(compressedDeviceHeader.get("fileUsed").intValue());
		cdh.putInt(compressedDeviceHeader.get("positionToFreeSpace").intValue());
		cdh.putInt(compressedDeviceHeader.get("totalFreeSpace").intValue());
		cdh.putInt(compressedDeviceHeader.get("largestFreeSpace").intValue());
		cdh.putInt(compressedDeviceHeader.get("numberFreeSpaces").intValue());
		cdh.putInt(compressedDeviceHeader.get("imbeddedFreeSpace").intValue());
		cdh.putInt(compressedDeviceHeader.get("noCylindersOnDevice").intValue());
		cdh.put(compressedDeviceHeader.get("nullTrackFormat").byteValue());
		cdh.put(compressedDeviceHeader.get("compressAlgorithm").byteValue());
		cdh.putShort(compressedDeviceHeader.get("compressParameter").shortValue());
		cdh.rewind();
		this.channel.position(512);
		this.channel.write(cdh);
	}

	public void sync() throws IOException {
		this.channel.force(true);
	}
}
