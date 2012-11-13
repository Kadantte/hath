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

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;

public class HHReqStatsFrame extends JFrame {
	private static final long serialVersionUID = 7956185908806079630L;
	private RequestStatsTableModel tblMdl;

	public HHReqStatsFrame() {
		Image icon32 = Toolkit.getDefaultToolkit().getImage(getClass().getResource("/src/org/hath/gui/icon32.png"));

		setTitle("Hentai@Home Request Monitor");
		setIconImage(icon32);
		setSize(600, 200);

		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		getContentPane().setLayout(new BorderLayout());

		JTable mainTbl = new JTable(tblMdl = new RequestStatsTableModel());

		mainTbl.getColumnModel().getColumn(0).setPreferredWidth(40);
		mainTbl.getColumnModel().getColumn(1).setPreferredWidth(100);
		mainTbl.getColumnModel().getColumn(2).setPreferredWidth(80);
		mainTbl.getColumnModel().getColumn(3).setPreferredWidth(80);
		mainTbl.getColumnModel().getColumn(4).setPreferredWidth(140);
		mainTbl.getColumnModel().getColumn(4).setCellRenderer(new ProgressBarTableCellRenderer());
		mainTbl.getColumnModel().getColumn(5).setPreferredWidth(60);

		JScrollPane scrollPane = new JScrollPane(mainTbl);
		scrollPane.setPreferredSize(new Dimension(600, 200));
		getContentPane().add(scrollPane, BorderLayout.CENTER);

		addWindowListener(new WindowAdapter() {
			@Override
			public void windowOpened(WindowEvent e) {
				tblMdl.activate();
			}
		});

		pack();
		setVisible(true);
	}

	@Override
	public void dispose() {
		tblMdl.dispose();

		super.dispose();
	}

	private class ProgressBarTableCellRenderer extends JProgressBar implements TableCellRenderer {
		private static final long serialVersionUID = 1957444417249315775L;

		public ProgressBarTableCellRenderer() {
			setBorderPainted(false);
			setMinimum(0);
			setMaximum(100);
		}

		public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
			if (value instanceof JComponent) {
				return (JComponent) value;
			} else if (value instanceof Integer) {
				this.setValue((Integer) value);
				return this;
			} else {
				return null;
			}
		}
	}
}
