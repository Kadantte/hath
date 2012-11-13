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

package org.hath.gui;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.swing.table.AbstractTableModel;

import org.hath.base.RequestStatListener;
import org.hath.base.RequestStats;

public class RequestStatsTableModel extends AbstractTableModel implements RequestStatListener {
	private static final long serialVersionUID = -1394722103746980127L;

	private static String[] columns = { "ID", "Client IP", "Bytes Sent", "Bytes Total", "Percent Complete", "Speed" };
	private static Class<?>[] columnCls = { int.class, String.class, String.class, String.class, int.class, String.class };

	private List<Integer> reqList = new ArrayList<Integer>();
	private ReadWriteLock reqListLock = new ReentrantReadWriteLock();

	private boolean listening = false;

	public RequestStatsTableModel() {
		reqList.addAll(RequestStats.getActiveRequestIds());
	}

	public void dispose() {
		if (listening)
			RequestStats.removeStatListener(this);
	}

	public void activate() {
		RequestStats.addStatListener(this);

		listening = true;
	}

	@Override
	public int getColumnCount() {
		return columns.length;
	}

	@Override
	public String getColumnName(int col) {
		return columns[col];
	}

	@Override
	public int getRowCount() {
		return RequestStats.getActiveRequestCount();
	}

	@Override
	public Object getValueAt(int rowIdx, int colIdx) {
		reqListLock.readLock().lock();
		int id;

		try {
			// Check to make sure we aren't requesting a row that doesn't exist.
			if (rowIdx >= reqList.size())
				return "";

			id = reqList.get(rowIdx);
		} finally {
			reqListLock.readLock().unlock();
		}

		RequestStats.Request r = RequestStats.getRequest(id);

		// Check again because concurrency sucks.
		if (r == null)
			return "";

		switch (colIdx) {
			case 0:
				return id;
			case 1:
				return r.getEndpoint();
			case 2:
				return String.format("%.2f KiB", r.getBytesSent() / 1024.0);
			case 3:
				return String.format("%.2f KiB", r.getBytesTotal() / 1024.0);
			case 4:
				return (int) Math.round(r.getPercentComplete() * 100);
			case 5:
				return String.format("%.2f KB/s", Math.round(r.getSpeedInKBps() * 100) / 100.0);
		}

		return "";

	}

	@Override
	public Class<?> getColumnClass(int colIdx) {
		return columnCls[colIdx];
	}

	@Override
	public void requestAdded(int id, String ep, int bytesTotal) {
		reqListLock.writeLock().lock();
		reqList.add(id);
		reqListLock.writeLock().unlock();

		fireTableRowsInserted(0, 0);
	}

	@Override
	public void requestCompleted(int id) {
		reqListLock.writeLock().lock();
		reqList.remove((Integer) id);
		reqListLock.writeLock().unlock();

		fireTableRowsDeleted(0, 0);
	}

	@Override
	public void requestUpdated(int id, int bytesSent) {
		reqListLock.readLock().lock();
		int idx = reqList.indexOf(id);
		reqListLock.readLock().unlock();

		if (idx > -1)
			fireTableRowsUpdated(idx, idx);
	}
}
