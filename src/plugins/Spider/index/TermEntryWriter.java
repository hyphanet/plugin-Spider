package plugins.Spider.index;

/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */


import java.util.Map;

import java.io.OutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
** Reads and writes {@link TermEntry}s in binary form, for performance.
**
** @author infinity0
*/
public class TermEntryWriter {

	final private static TermEntryWriter instance = new TermEntryWriter();

	protected TermEntryWriter() {}

	public static TermEntryWriter getInstance() {
		return instance;
	}

	/*@Override**/ public void writeObject(TermEntry en, OutputStream os) throws IOException {
		writeObject(en, new DataOutputStream(os));
	}
	
	public void writeObject(TermEntry en, DataOutputStream dos) throws IOException {
		dos.writeLong(TermEntry.serialVersionUID2);
		TermEntry.EntryType type = en.entryType();
		dos.writeInt(type.ordinal());
		dos.writeUTF(en.subj);
		dos.writeFloat(en.rel);
		switch (type) {
		case PAGE:
			TermPageEntry enn = (TermPageEntry)en;
			dos.writeUTF(enn.page.toString());
			if(enn.title == null)
				dos.writeBoolean(false);
			else {
				dos.writeBoolean(true);
				dos.writeUTF(enn.title);
			}
			int size = enn.hasPositions() ? enn.positionsSize() : 0;
			dos.writeInt(size);
			if(size != 0) {
				if(enn.hasFragments()) {
					for(Map.Entry<Integer, String> p : enn.positionsMap().entrySet()) {
						dos.writeInt(p.getKey());
						if(p.getValue() == null)
							dos.writeUTF("");
						else
							dos.writeUTF(p.getValue());
					}
				} else {
					for(int x : enn.positionsRaw()) {
						dos.writeInt(x);
						dos.writeUTF("");
					}
				}
			}
			return;
		default:
			throw new RuntimeException("Not implemented");
		}
	}

}
