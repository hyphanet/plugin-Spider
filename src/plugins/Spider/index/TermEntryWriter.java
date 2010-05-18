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
		dos.writeLong(TermEntry.serialVersionUID);
		TermEntry.EntryType type = en.entryType();
		dos.writeInt(type.ordinal());
		dos.writeUTF(en.subj);
		dos.writeFloat(en.rel);
		switch (type) {
		case PAGE:
			TermPageEntry enn = (TermPageEntry)en;
			enn.page.writeFullBinaryKeyWithLength(dos);
			int size = enn.hasPositions() ? enn.positionsSize() : 0;
			if(enn.title == null)
				dos.writeInt(size);
			else {
				dos.writeInt(~size); // invert bits to signify title is set
				dos.writeUTF(enn.title);
			}
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
		}
	}

}
