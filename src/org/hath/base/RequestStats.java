/*
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RequestStats {
	private static Map<Integer, Request> conns = new HashMap<Integer, Request>();
	private static List<RequestStatListener> listeners = new ArrayList<RequestStatListener>();

	private RequestStats() throws IllegalAccessException {
		throw new IllegalAccessException("Don't instantiate me please.");
	}

	public static void addStatListener(RequestStatListener listener) {
		synchronized (listeners) {
			if (!listeners.contains(listener)) {
				listeners.add(listener);
			}
		}
	}

	public static void removeStatListener(RequestStatListener listener) {
		synchronized (listeners) {
			listeners.remove(listener);
		}
	}

	public static void newRequest(int id, String ip, int bytesTotal) {
		synchronized (conns) {
			conns.put(id, new Request(id, ip, bytesTotal));
		}

		for (RequestStatListener l : listeners)
			l.requestAdded(id, ip, bytesTotal);
	}

	public static void finishedRequest(int id) {
		boolean removed;

		synchronized (conns) {
			removed = conns.remove(id) != null;
		}

		if (removed)
			for (RequestStatListener l : listeners)
				l.requestCompleted(id);
	}

	public static void updateRequestStatus(int id, int bytesSent) {
		Request r;
		synchronized (conns) {
			r = conns.get(id);
		}

		if (r != null)
			r.setBytesSent(bytesSent);

		for (RequestStatListener l : listeners)
			l.requestUpdated(id, bytesSent);
	}

	public static int getActiveRequestCount() {
		return conns.size();
	}

	public static Request getRequest(int id) {
		return conns.get(id);
	}

	public static List<Integer> getActiveRequestIds() {
		List<Integer> l = new ArrayList<Integer>();
		synchronized (conns) {
			l.addAll(conns.keySet());
		}

		Collections.sort(l);
		return l;
	}

	public static class Request {
		private int id;
		private String endpoint;
		private int bytesSent, bytesTotal;
		private long startTime = System.currentTimeMillis();

		public Request(int id, String ep, int bytesTotal) {
			this.id = id;
			this.endpoint = ep;
			this.bytesSent = 0;
			this.bytesTotal = bytesTotal;
		}

		public int getId() {
			return id;
		}

		public String getEndpoint() {
			return endpoint;
		}

		public int getBytesSent() {
			return bytesSent;
		}

		public int getBytesTotal() {
			return bytesTotal;
		}

		public double getPercentComplete() {
			return (double) bytesSent / bytesTotal;
		}

		public double getSpeedInKBps() {
			return bytesSent / 1024.0 / (System.currentTimeMillis() - startTime) * 1000;
		}

		public void setBytesSent(int sent) {
			this.bytesSent = sent;
		}
	}
}
