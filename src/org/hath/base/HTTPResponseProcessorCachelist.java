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

package org.hath.base;

import java.util.LinkedList;

import org.hath.base.CacheHandler.CacheListFile;

public class HTTPResponseProcessorCachelist extends HTTPResponseProcessor {
	private CacheHandler cacheHandler;
	private int max_filesize, max_filecount;

	private int cacheListStrlen;
	private StringBuilder fileidBuffer;
	private int fileidOffset, fileidCount;
	private int bufferFilenum;

	public HTTPResponseProcessorCachelist(CacheHandler cacheHandler, int max_filesize, int max_filecount) {
		this.cacheHandler = cacheHandler;
		this.max_filesize = max_filesize <= 0 ? 10485760 : max_filesize;
		this.max_filecount = max_filecount;
		this.bufferFilenum = Settings.isUseMoreMemory() ? 25000 : 1000;
	}

	public int initialize() {
		Out.info("Calculating cache file list parameters and preparing for send...");
		cacheListStrlen = cacheHandler.getCachedFilesStrlen(max_filesize, max_filecount);
		Out.debug("Calculated cacheListStrlen = " + cacheListStrlen);

		fileidBuffer = new StringBuilder(65 * bufferFilenum);
		fileidOffset = 0;
		fileidCount = 0;

		Out.info("Sending cache list, and waiting for the server to register the cached files.. (this could take a while)");

		return 200;
	}

	public int getContentLength() {
		return cacheListStrlen;
	}

	public byte[] getBytes() {
		return getBytesRange(cacheListStrlen);
	}

	public byte[] getBytesRange(int len) {
		while (fileidBuffer.length() < len) {
			int buflen = Math.min(Math.min(cacheHandler.getCacheCount(), max_filecount) - fileidCount, bufferFilenum);

			LinkedList<CacheListFile> cachedFiles = cacheHandler.getCachedFilesRange(fileidOffset, buflen);
			if (cachedFiles.size() < 1) {
				HentaiAtHomeClient.dieWithError("Failed to buffer requested data for cache list write. (fileidBuffer=" + fileidBuffer.length() + ", len=" + len + ", max_filecount=" + max_filecount + ", max_filesize=" + max_filesize + ", fileidOffset=" + fileidOffset + ", buflen=" + buflen + ")");
			}

			for (CacheListFile file : cachedFiles) {
				if (file.getFilesize() <= max_filesize) {
					fileidBuffer.append(file.getFileid() + "\n");
					++fileidCount;
				}
			}

			fileidOffset += buflen;
		}

		byte[] returnBytes = fileidBuffer.substring(0, len).getBytes(java.nio.charset.Charset.forName("ISO-8859-1"));
		fileidBuffer.delete(0, len);

		if (returnBytes.length != len) {
			HentaiAtHomeClient.dieWithError("Length of cache list buffer (" + returnBytes.length + ") does not match requested length (" + len + ")! Bad program!");
		}

		return returnBytes;
	}

}
