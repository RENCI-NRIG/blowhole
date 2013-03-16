package org.renci.pubsub_daemon;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

public class SystemExecutor {
	Logger l = null;

	public SystemExecutor(Logger l) {
		this.l = l;
	}

	public SystemExecutor() {
		this.l = Logger.getLogger(this.getClass());
	}

	public String execute(List<String> cmd, Properties newEnv, String wd, Reader sendToProcess) {
		ProcessBuilder pb = new ProcessBuilder(cmd);
		Map<String, String> pEnv = pb.environment();

		// add or substitute properties to environment variables
		if (newEnv != null) {
			for (Enumeration<?> propKeys = newEnv.propertyNames(); propKeys.hasMoreElements();) {
				String prop = (String)propKeys.nextElement();
				String oldVal = pEnv.get(prop);
				if (null != oldVal) {
					pEnv.put(prop, newEnv.getProperty(prop) + ":" + oldVal);
				}
				else {
					pEnv.put(prop, newEnv.getProperty(prop));
				}
				if (l != null) 
					l.debug(prop + " has been set to " + pEnv.get(prop));
				else
					System.out.println(prop + " has been set to " + pEnv.get(prop));
			}
		}
		if (wd != null)
			pb.directory(new File(wd));
		
		Process p;
		int exitValue = 0;
		StringBuilder accumulator = new StringBuilder(); 

		try {

			p = pb.start();

			OutputStream processInput = p.getOutputStream();
			BufferedWriter bufProcessInputWriter = new BufferedWriter(new OutputStreamWriter(processInput));

			InputStream processOutput = p.getInputStream();
			BufferedReader bufProcessOutputReader = new BufferedReader(new InputStreamReader(processOutput));

			InputStream processError = p.getErrorStream();	
			BufferedReader bufProcessErrorReader = new BufferedReader(new InputStreamReader(processError));

			if (sendToProcess != null) {
				BufferedReader bufSendToProcess = new BufferedReader(sendToProcess);

				int send;
				// send to the process stdin. could've use a thread,
				// but not necessary here	
				char [] cbuf = new char[1024];
				while((send = bufSendToProcess.read(cbuf)) != -1) 
					bufProcessInputWriter.write(cbuf, 0, send);
			}

			bufProcessInputWriter.close();

			// receive result from process stdout
			String ret;

			try {
				while((ret = bufProcessOutputReader.readLine()) != null) {
					accumulator.append(ret);
				}
			} catch (IOException e) {
				if (l != null)
					l.error(e);
				else
					System.err.println(e);
			}

			exitValue = p.waitFor();

			if (exitValue != 0) {
				// read stderror
				try {
					accumulator = new StringBuilder(); 
					while((ret = bufProcessErrorReader.readLine()) != null) {
						accumulator.append(ret);
					}
				} catch (IOException e) {
					if (l != null)
						l.error(e);
					else
						System.err.println(e);
				}
				throw new RuntimeException("Command " + pb.command().toString() + " returned exit code " + exitValue + " and error message: \n" + 
						accumulator.toString());
			}
		} catch (Exception e) {
			if (l != null) {
				l.error(e);
				throw new RuntimeException(e);
			}
			else
				System.err.println(e);
		}

		return accumulator.toString();
	}

	public byte[] execute(List<String> cmd, Properties newEnv, String wd, byte[] sendToProcess) {
		ProcessBuilder pb = new ProcessBuilder(cmd);
		Map<String, String> pEnv = pb.environment();
		ByteArrayOutputStream ret = new ByteArrayOutputStream();

		// add or substitute properties to environment variables
		if (newEnv != null) {
			for (Enumeration<?> propKeys = newEnv.propertyNames(); propKeys.hasMoreElements();) {
				String prop = (String)propKeys.nextElement();
				String oldVal = pEnv.get(prop);
				if (null != oldVal) {
					pEnv.put(prop, newEnv.getProperty(prop) + ":" + oldVal);
				}
				else {
					pEnv.put(prop, newEnv.getProperty(prop));
				}
				if (l != null) 
					l.debug(prop + " has been set to " + pEnv.get(prop));
				else
					System.out.println(prop + " has been set to " + pEnv.get(prop));
			}
		}
		Process p;
		int exitValue = 0;

		if (wd != null)
			pb.directory(new File(wd));
		
		try {

			p = pb.start();

			OutputStream processInput = p.getOutputStream();
			BufferedOutputStream bufferedProcessInput = new BufferedOutputStream(processInput);

			InputStream processOutput = p.getInputStream();
			BufferedInputStream bufferedProcessOutput = new BufferedInputStream(processOutput);

			InputStream processError = p.getErrorStream();	
			BufferedInputStream bufferedProcessErrorOutput = new BufferedInputStream(processError);
			
			if (sendToProcess != null) {
				bufferedProcessInput.write(sendToProcess);
				bufferedProcessInput.close();
			}

			// receive result from process stdout
			int readBytes=0;
			byte[] tmp = new byte[1024];

			try {
				while ((readBytes=bufferedProcessOutput.read(tmp)) != -1) {
					ret.write(tmp, 0, readBytes);
				}
			} catch (IOException e) {
				if (l != null)
					l.error(e);
				else
					System.err.println(e);
			}

			// get exit value 
			exitValue = p.waitFor();

			if (exitValue != 0) {
				// read stderror
				try {
					ret = new ByteArrayOutputStream();
					while ((readBytes=bufferedProcessErrorOutput.read(tmp)) != -1) {
						ret.write(tmp, 0, readBytes);
					}
				} catch (IOException e) {
					if (l != null)
						l.error(e);
					else
						System.err.println(e);
				}
				throw new RuntimeException("Command " + pb.command().toString() + " returned exit code " + exitValue + " and error message: \n" +
						ret.toByteArray());
			}
		} catch (Exception e) {
			if (l != null) {
				l.error(e);
				throw new RuntimeException(e);
			}
			else
				System.err.println(e);
		}

		return ret.toByteArray();
	}

	//	public static void main(String argv[]) {
	//		SystemExecutor s = new SystemExecutor();
	//		
	//		String toSend = "Sending this string\n to the process\n to see what happens";
	//		StringReader sr = new StringReader(toSend);
	//		
	//		List<String> myCommand = new ArrayList<String>();
	//		myCommand.add("ls");
	//		myCommand.add("-l");
	//		 
	//		Properties newEnv = new Properties();
	//		newEnv.setProperty("PATH", "/Users/ibaldin/workspace/orca.build-all/ndl-conversion/scripts/ns2ir");
	//		
	//		byte[] ret = s.execute(myCommand, newEnv, toSend.getBytes());
	//		
	//		String retString = new String(ret);
	//		System.out.println("Return code is " + retString);
	//	}

}