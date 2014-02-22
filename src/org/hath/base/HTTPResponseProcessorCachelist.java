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
	private int max_filesize;

	private int cacheListStrlen, cacheListWritten;
	private StringBuilder fileidBuffer;
	private int fileidOffset;
	private int bufferFilenum;

	public HTTPResponseProcessorCachelist(CacheHandler cacheHandler, int max_filesize) {
		this.cacheHandler = cacheHandler;
		this.max_filesize = max_filesize <= 0 ? 10485760 : max_filesize;
		this.bufferFilenum = Settings.isUseLessMemory() ? 1000 : 25000;
	}

	@Override
	public int initialize() {
		Out.info("Calculating cache file list parameters and preparing for send...");
		cacheListStrlen = cacheHandler.getCachedFilesStrlen(max_filesize);
		cacheListWritten = 0;
		Out.debug("Calculated cacheListStrlen = " + cacheListStrlen);

		fileidBuffer = new StringBuilder(65 * bufferFilenum);
		fileidOffset = 0;

		Out.info("Sending cache list, and waiting for the server to register the cached files.. (this could take a while)");

		return 200;
	}

	@Override
	public int getContentLength() {
		return cacheListStrlen;
	}

	@Override
	public byte[] getBytes() {
		return getBytesRange(cacheListStrlen);
	}

	@Override
	public byte[] getBytesRange(int len) {
		// Out.debug("Before: cacheListStrlen=" + cacheListStrlen + ", cacheListWritten=" + cacheListWritten +
		// ", fileidBuffer=" + fileidBuffer.length() +", len=" + len + ", max_filesize=" + max_filesize +
		// ", fileidOffset=" + fileidOffset);

		while (fileidBuffer.length() < len) {
			LinkedList<CacheListFile> cachedFiles = cacheHandler.getCachedFilesRange(fileidOffset, bufferFilenum);

			if (cachedFiles.size() < 1) {
				HentaiAtHomeClient.dieWithError("Failed to buffer requested data for cache list write. (cacheListStrlen=" + cacheListStrlen + ", cacheListWritten=" + cacheListWritten + ", fileidBuffer=" + fileidBuffer.length() + ", len=" + len + ", max_filesize=" + max_filesize + ", fileidOffset=" + fileidOffset + ")");
			}

			for (CacheListFile file : cachedFiles) {
				if ((file.getFilesize() <= max_filesize) && !Settings.isStaticRange(file.getFileid())) {
					fileidBuffer.append(file.getFileid() + "\n");
				}
			}

			fileidOffset += bufferFilenum;
		}

		byte[] returnBytes = fileidBuffer.substring(0, len).getBytes(java.nio.charset.Charset.forName("ISO-8859-1"));
		fileidBuffer.delete(0, len);

		if (returnBytes.length != len) {
			HentaiAtHomeClient.dieWithError("Length of cache list buffer (" + returnBytes.length + ") does not match requested length (" + len + ")! Bad program!");
		}

		cacheListWritten += returnBytes.length;

		// Out.debug("After: cacheListStrlen=" + cacheListStrlen + ", cacheListWritten=" + cacheListWritten +
		// ", fileidBuffer=" + fileidBuffer.length() +", len=" + len + ", max_filesize=" + max_filesize +
		// ", fileidOffset=" + fileidOffset);

		return returnBytes;
	}

}
