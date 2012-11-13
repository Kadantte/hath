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

public class HTTPResponseProcessorSpeedtest extends HTTPResponseProcessor {
	int testsize = 0;

	public HTTPResponseProcessorSpeedtest(int testsize) {
		this.testsize = testsize;
	}

	public int getContentLength() {
		return testsize;
	}

	public byte[] getBytes() {
		return getRandomBytes(testsize);
	}

	public byte[] getBytesRange(int len) {
		return getRandomBytes(len);
	}

	private byte[] getRandomBytes(int testsize) {
		// generate a random body the server can use to gauge the actual upload speed capabilities of this client
		byte[] random = new byte[testsize];
		for (int i = 0; i < testsize; i++) {
			random[i] = (byte) Math.floor(Math.random() * 256);
		}
		return random;
	}
}
