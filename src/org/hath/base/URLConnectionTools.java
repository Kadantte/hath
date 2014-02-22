/*

Copyright 2008-2012 E-Hentai.org
http://forums.e-hentai.org/
ehentai@gmail.com

This file is part of Hentai@Home.

Hentai@Home is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Hentai@Home is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Hentai@Home.  If not, see <http://www.gnu.org/licenses/>.

*/

// incredibly old code adopted from an old project, with some additional protections added.
// maybe look for a port of cURL to java or something? todo...

package org.hath.base;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class URLConnectionTools {

	private static final int TIMEOUT = 30000;
	private static final int MAX_DLTIME = Integer.MAX_VALUE; // H@H note: used to pummel hostile clients who
																// intentionally throttles speed to very low speeds to
																// tie up stuff.
	private static final int RETRIES = 3;

	private static String forcedHost;

	static {
		// *hack* *hack*
		forcedHost = Settings.getClientHost();
		if (forcedHost.length() < 1) {
			forcedHost = null;
		}
	}

	public static boolean saveFile(URL source, File destination) {
		return saveFile(source, destination, TIMEOUT, MAX_DLTIME);
	}

	public static boolean saveFile(URL source, File destination, int timeout) {
		return saveFile(source, destination, timeout, MAX_DLTIME);
	}

	public static boolean saveFile(URL source, File destination, int timeout, int maxDLTime) {
		if (destination.exists()) {
			destination.delete();
		}

		FileOutputStream fos = null;

		try {
			FileTools.checkAndCreateDir(destination.getParentFile());

			byte[] bytearray = downloadFile(source, timeout, maxDLTime);

			if (bytearray != null) { // only if downloadFile is successful
				fos = new FileOutputStream(destination);
				fos.write(bytearray, 0, bytearray.length);
				fos.close();
				bytearray = null;

				return true;
			}
		} catch (Exception e) {
			try {
				fos.close();
			} catch (Exception e2) {
			} // nuke file handle if open

			if (e instanceof java.io.IOException && e.getMessage().equals("There is not enough space on the disk")) {
				Out.warning("Error: No space on disk");
			}
			else {
				Out.warning(e + " while saving file " + source + " to " + destination.getAbsolutePath());
				e.printStackTrace();
			}
		}

		return false;
	}

	public static String getTextContent(URL source) {
		return getTextContent(source, TIMEOUT, MAX_DLTIME);
	}

	public static String getTextContent(URL source, int timeout) {
		return getTextContent(source, timeout, MAX_DLTIME);
	}

	public static String getTextContent(URL source, int timeout, int maxDLTime) {
		byte[] bytearray = downloadFile(source, timeout, maxDLTime);
		if (bytearray != null) {
			return new String(bytearray, 0, bytearray.length);
		}
		else {
			return null;
		}
	}

	// XXX unused
	@SuppressWarnings("unused")
	private static byte[] downloadFile(URL source) {
		return downloadFile(source, TIMEOUT, MAX_DLTIME);
	}

	// XXX unused
	@SuppressWarnings("unused")
	private static byte[] downloadFile(URL source, int timeout) {
		return downloadFile(source, timeout, MAX_DLTIME);
	}

	private static byte[] downloadFile(URL source, int timeout, int maxDLTime) {
		int trycounter = RETRIES;

		while (trycounter-- > 0) {
			InputStream is = null;
			BufferedInputStream bis = null;

			try {
				Out.info("Connecting to " + source.getHost() + "...");

				URLConnection connection = source.openConnection();

				/*
				if(forcedHost == null) {
					connection = source.openConnection();
				}
				else {
					Proxy proxy = new Proxy(Proxy.Type.DIRECT, new InetSocketAddress(InetAddress.getByName(forcedHost), 0));
					connection = source.openConnection(proxy);
				}
				*/

				connection.setConnectTimeout(10000);
				connection.setReadTimeout(timeout); // this doesn't always seem to work however, so we'll do it somewhat
													// differently..
				connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080201 Firefox/2.0.0.12");
				connection.connect();

				int contentLength = connection.getContentLength();

				if (contentLength < 0) {
					// H@H note: since we control all systems in this case, we'll demand that clients and servers always
					// send the Content-Length
					// (code to handle missing content length removed, look in lib if necessary)
					Out.warning("Remote host did not send Content-Length, aborting transfer.");
					return null;
				}
				else if (contentLength > 10485760) {
					// H@H note: we don't want clients trying to provoke an outofmemory exception by returning a
					// malformed oversized reply, so we'll limit the download size to the H@H max (10 MB). the server
					// will never send anything this large as a response either.
					Out.warning("Reported contentLength " + contentLength + " on request " + source + " is out of bounds!");
					return null;
				}

				byte[] bytearray = new byte[contentLength];
				is = connection.getInputStream();
				bis = new BufferedInputStream(is);

				Out.info(source.getPath() + (source.getQuery() != null ? "?" + source.getQuery() : "") + ": Retrieving " + contentLength + " bytes...");
				long downloadStart = System.currentTimeMillis();

				int bytecounter = 0; // counts the number of bytes read
				int time = 0; // counts the approximate time (in nanofortnights) since last byte was received

				// note: this may seen unnecessarily hackjob-ish, but because the built-in timeouts were unreliable at
				// best (at the time of testing), this was a way to deal with the uncertainties of the interwebs. not
				// exactly C10K stuff, but it works.
				while (bytecounter < contentLength) {
					if (bis.available() > 0) {
						// read-data loop..

						time = 0;
						int b = bis.read();

						if (b >= 0) {
							bytearray[bytecounter++] = (byte) b;
						}
						else {
							// b == -1 => EOF
							Out.warning("\nServer sent premature EOF, aborting.. (" + bytecounter + " of " + contentLength + " bytes received)");
							throw new java.net.SocketException("Unexpected end of file from server");
						}
					}
					else {
						// wait-for-data loop...

						if (System.currentTimeMillis() - downloadStart > MAX_DLTIME) {
							Out.warning("\nDownload time limit has expired, aborting...");
							throw new java.net.SocketTimeoutException("Download timed out");
						}
						else if (time > timeout) {
							Out.warning("\nTimeout detected waiting for byte " + bytecounter + ", aborting..");
							throw new java.net.SocketTimeoutException("Read timed out");
						}

						time += 5;
						Thread.sleep(5);
					}
				}

				bis.close();
				is.close();

				Stats.bytesRcvd(contentLength);

				return bytearray;
			} catch (Exception e) {
				try {
					bis.close();
				} catch (Exception e2) {
				}
				try {
					is.close();
				} catch (Exception e3) {
				}

				String message = e.getMessage();
				Throwable cause = e.getCause();
				String causemessage = null;
				if (cause != null)
					causemessage = (cause.getMessage() != null) ? cause.getMessage() : "";

				if (message != null) {
					if (message.equals("Request timed out: connect")) {
						Out.warning("Request timed out getting " + source + ", retrying.. (" + trycounter + " tries left)");
						continue;
					}
					else if (message.equals("Request refused: connect")) {
						Out.warning("Request refused getting " + source + ", retrying.. (" + trycounter + " tries left)");
						continue;
					}
					else if (message.equals("Unexpected end of file from server")) {
						Out.warning("Request prematurely reset getting " + source + ", retrying.. (" + trycounter + " tries left)");
						continue;
					}
					else if (e instanceof java.io.FileNotFoundException) {
						Out.warning("Server returned: 404 Not Found");
						break;
					}
					else if (message.indexOf("403 for URL") >= 0) {
						Out.warning("Server returned: 403 Forbidden");
						break;
					}
					else if (e instanceof java.net.SocketException && message.equals("Request reset")) {
						Out.warning("Request reset getting " + source + ", retrying.. (" + trycounter + " tries left)");
						continue;
					}
					else if (e instanceof java.net.UnknownHostException) {
						Out.warning("Unknown host " + source.getHost() + ", aborting..");
						break;
					}
					else if (e instanceof java.net.SocketTimeoutException) {
						Out.warning("Read timed out, retrying.. (" + trycounter + " tries left)");
						continue;
					}
					else {
						Out.warning("Unhandled exception: " + e.toString());
						e.printStackTrace();
						Out.warning("Retrying.. (" + trycounter + " tries left)");
						continue;
					}
				}
				else if (cause != null) {
					if (cause instanceof java.io.FileNotFoundException) {
						Out.warning("Server returned: 404 Not Found");
						break;
					}
					else if (causemessage.indexOf("403 for URL") >= 0) {
						Out.warning("Server returned: 403 Forbidden");
						break;
					}
					else if (causemessage.equals("Unexpected end of file from server")) {
						Out.warning("Request prematurely reset getting " + source + ", retrying.. (" + trycounter + " tries left)");
						continue;
					}
					else {
						Out.warning("Unhandled exception/cause: " + e.toString());
						e.printStackTrace();
						Out.warning("Retrying.. (" + trycounter + " tries left)");
						continue;
					}
				}
				else {
					Out.warning("Exception with no exception message nor cause:");
					e.printStackTrace();
					Out.warning("Retrying.. (" + trycounter + " tries left)");
					continue;
				}
			}
		}

		Out.warning("Exhaused retries or aborted getting " + source);
		return null;
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Need source and destination.");
		}
		else {
			try {
				URL source = new URL(args[0]);
				File dest = new File(args[1]);

				System.out.println("Downloading file " + source);

				saveFile(source, dest);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}