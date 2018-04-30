package ToltecDatabase;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import Tools.Log;
import Tools.MurmurHash;

public class DataFileStoreSchema extends DataFile {
	public DataFileStoreSchema(String fileName, String folderName, boolean isFinal) {
		super(fileName, folderName,  isFinal);
	}
	public DataFileStoreSchema(String fileName, boolean isFinal) {
		super(fileName, isFinal);
	}

	DataFile m_schemaTable;

	public void init(String fileName, String indexFolder) {
		super.init(fileName,indexFolder);
		m_schemaTable = new DataFile(fileName + ".schema", false);
		m_schemaTable.addColumn("type", TableSchema.TYPE_INT, false,false);
		m_schemaTable.addColumn("name", TableSchema.TYPE_STRING, false,false);
		m_schemaTable.addColumn("index", TableSchema.TYPE_INT, false,false);
		m_schemaTable.addColumn("sorted", TableSchema.TYPE_INT, false,false);
		ArrayList<DataRow> objs = m_schemaTable.readAll();
		for (HashMap<String, Object> hashMap : objs) {
			boolean isIndex = (Integer) hashMap.get("index") == 1;
			boolean isSorted = (Integer) hashMap.get("sorted") == 1;
			super.addColumn((String) hashMap.get("name"), (Integer) hashMap.get("type"), isIndex,isSorted);
		}	
	}
		
	public DataFile getSchemaDataFile () {
		return m_schemaTable;
	}
	
	public void close () {
		super.close();
		m_schemaTable.close();
	}
	
	public void remove () {
		super.remove();
		m_schemaTable.remove();
	}
	
	public void rename (String name) {
		super.rename(name);
		m_schemaTable.rename(name+".schema");
	}
	
	
	public boolean addColumn(String name, int type, boolean isIndex, boolean isSorted) {
		if (m_schema.m_tableTypesWithNames.get(name) != null){
			Log.message("addColumn, name:"+name+" is exist.");
			return false;
		}
		if (count()>0){
			Log.warning("Can't add columns after filling database.");
			return false;
		}
		if (!super.addColumn(name, type, isIndex,isSorted)) {
			return false;
		}
		DataRow col = new DataRow();
		col.put("type", type);
		col.put("name", name);
		if (isIndex)
			col.put("index", 1);
		else
			col.put("index", 0);
		
		if (isSorted)
			col.put("sorted", 1);
		else
			col.put("sorted", 0);
		
		m_schemaTable.systemAddData(col);
		Log.message ("addColumn name:"+name+" success.");
		return true;
	}
}
