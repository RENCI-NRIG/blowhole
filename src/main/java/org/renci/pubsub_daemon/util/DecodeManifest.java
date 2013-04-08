package org.renci.pubsub_daemon.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.zip.DataFormatException;

import org.renci.pubsub_daemon.CompressEncode;

/**
 * Helper program to decode manifests saved in db
 * @author ibaldin
 *
 */
public class DecodeManifest {
	private String man;

	public DecodeManifest(String manFile, String newFile) {
		if ((manFile == null) || (newFile == null)) {
			System.err.println("Unvalid file name parameters");
			return;
		}
		try {
			man = readFile(manFile);
		} catch (IOException e) {
			System.err.println("Error reading manifest file " + manFile);
		}
		// skip the first line with slice name
		int ind = man.indexOf('\n');
		String decoded;
		if ((ind > 0) && (ind < 100))
			decoded = decode(man.substring(ind));
		else
			decoded = decode(man);
		// write to new file
		if (newFile.equals("-")) {
			System.out.println(decoded);
		} else {
			try {
				PrintWriter pw = new PrintWriter(new File(newFile));
				pw.write(decoded);
				pw.close();
			} catch (Exception e) {
				System.err.println("Unable to save decoded manifest to " + newFile);
			}
		}
	}

	private static String readFile(String path) throws IOException {
		FileInputStream stream = new FileInputStream(new File(path));
		try {
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
			/* Instead of using default, pass in a decoder. */
			return Charset.defaultCharset().decode(bb).toString();
		} 
		finally {
			stream.close();
		}
	}

	String decode(String m) {
		String ndlMan = null;
		try {
			ndlMan = CompressEncode.decodeDecompress(m);
		} catch (DataFormatException dfe) {
			System.err.println("Error: Unable to decode manifest");
		}
		return ndlMan;
	}

	public static void main(String argv[]) {
		if (argv.length != 2) {
			System.err.println("Usage: decompress  <compressed manifest> -|<uncompressed manifest>");
			System.exit(1);
		}

		DecodeManifest dm = new DecodeManifest(argv[0], argv[1]);
	}
}
