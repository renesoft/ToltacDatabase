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

import Tools.MurmurHash;

public class DataFileStoreSchema extends DataFile {
	public DataFileStoreSchema(String fileName, String folderName, boolean isFinal) {
		super(fileName, folderName,  isFinal);
	}
	public DataFileStoreSchema(String fileName, boolean isFinal) {
		super(fileName, isFinal);
	}

	DataFile schemaTable;

	public void init(String fileName, String indexFolder) {
		super.init(fileName,indexFolder);
		schemaTable = new DataFile(fileName + ".schema", false);
		schemaTable.addColumn("type", TableSchema.TYPE_INT, false,false);
		schemaTable.addColumn("name", TableSchema.TYPE_STRING, false,false);
		schemaTable.addColumn("index", TableSchema.TYPE_INT, false,false);
		schemaTable.addColumn("sorted", TableSchema.TYPE_INT, false,false);
		ArrayList<DataRow> objs = schemaTable.readAll();
		for (HashMap<String, Object> hashMap : objs) {
			boolean isIndex = (Integer) hashMap.get("index") == 1;
			boolean isSorted = (Integer) hashMap.get("sorted") == 1;
			super.addColumn((String) hashMap.get("name"), (Integer) hashMap.get("type"), isIndex,isSorted);
		}
		
		/*if (this.m_fileWorker.sizeBytes()>4){
			this.m_fileWorker.goTo(this.m_fileWorker.sizeBytes()-4);
			int endR = this.m_fileWorker.readInt();
			if (endR!= ToltecDatabase.m_endCode){
				System.out.println("Something error!");
				for (long i = this.m_fileWorker.sizeBytes()-4; i>=4; i-- ){
					System.out.println("goto "+i);
					this.m_fileWorker.goTo(i);
					int beginR = this.m_fileWorker.readInt();
					if (beginR == ToltecDatabase.m_beginCode){
						System.out.println("Founded begin");
						this.m_fileWorker.goTo(i-4);
						int size = this.m_fileWorker.readInt();
						long s = this.m_fileWorker.sizeBytes() - (size+i);
						System.out.println("size + i = "+(size+i)+" /" + this.m_fileWorker.sizeBytes() + " = losted "+s+ "bytes" );
						HashMap <String, Object> o = new HashMap<>();
						try{
							int r = this.readData(o);
						}catch (Exception e){
							e.printStackTrace();
						}
						
					}
				}
			}else{
				System.out.println("check fine.");
			}
		}*/
		
		
	}
	
	
	public void close () {
		super.close();
		schemaTable.close();
	}
	
	public void remove () {
		super.remove();
		schemaTable.remove();
	}
	
	public void rename (String name) {
		super.rename(name);
		schemaTable.rename(name+".schema");
	}
	
	
	public void log (String text){
		String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis()));
		System.out.println(date + ":[DataFileStoreSchema] "+text);
	}
	
	public boolean addColumn(String name, int type, boolean isIndex, boolean isSorted) {
		if (m_schema.m_tableTypesWithNames.get(name) != null){
			log ("addColumn, name:"+name+" is exist.");
			return false;
		}
		if (count()>0){
			System.out.println("Can't add columns after filling database.");
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
		
		schemaTable.systemAddData(col);
		log ("addColumn name:"+name+" success.");
		return true;
	}
	public static DataFileStoreSchema file ;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		file = new DataFileStoreSchema("C:/tmp/DataFileStoreSchema.db", false);

		int c = 0 ;
		
		// file.databaseDump("");

		file.addColumn("company_name", TableSchema.TYPE_STRING, true,false);
		file.addColumn("id", TableSchema.TYPE_INT, false,false);
		file.addColumn("hash", TableSchema.TYPE_INT, false,false);
		int cc = 0;
//		file.schemaTable.databaseDump("");
		

		
		
	if (file.count() == 0) {
		load();			
	}
	/*[row 1]:offset:118461;company_name:Demand Energy, Inc.;id:2957;hash:2024711032;
[row 2]:offset:363413;company_name:Demand Energy, Inc.;id:9111;hash:1373731218;
[row 3]:offset:1130139;company_name:Demand Energy, Inc.;id:28105;hash:1183280512;
[row 4]:offset:1561564;company_name:Demand Energy, Inc.;id:39003;hash:937041216;
[row 5]:offset:2645748;company_name:Demand Energy, Inc.;id:66596;hash:-2140965309;
[row 6]:offset:2808079;company_name:Demand Energy, Inc.;id:70712;hash:-2111339644;*/	

	//readAllRecorda();
	
	
		/*System.out.println("================================================================");
		HashMap<String, Object> col1 = new HashMap<>();
		col1.put("name", "company");
		file.schemaTable.updateData(col1,"name","company_name");		
		file.schemaTable.databaseDump("");
		System.out.println("================================================================");
		HashMap<String, Object> col2 = new HashMap<>();
		col2.put("name", "company_nam");
		file.schemaTable.updateData(col1,"name","company");		
		file.schemaTable.databaseDump("");*/
		
		
		
		
		c = 0 ;
		/*if (file.getIndex("company_name")!=null){
			if (file.getIndex("company_name").m_isSorted==false){
				file.getIndex("company_name").sort();
				HashMap<String, Object> col = new HashMap<>();
				col.put("sorted", 1);
				file.schemaTable.updateData(col,"name","company_name");		
				file.schemaTable.databaseDump("");
			}
			//indexFile.analize();			
			//indexFile.m_isSorted=true;
			//indexFile.sort();
		}*/
		
		//
		//file.getIndex("company_name").sort();	
		readBy("Demand Energy AZAZA");
		System.out.println("Index count:"+file.getIndex("company_name").countElements());
		readDemandEnergy();
		DataRow upd = new DataRow();
		upd.put("company_name", "Demand Energy AZAZA");
		file.updateData(upd, "company_name", "Demand Energy, Inc.");
		//file.getIndex("company_name").sort();
		System.out.println("Index count after:"+file.getIndex("company_name").countElements());
		
		/**/
		
		/*ArrayList<HashMap<String, Object>> objs2 = file.getData("company_name", "Demand Energy, Inc.");
		for (HashMap<String, Object> hashMap : objs2) {
			c++;
			System.out.println("===== row "+c+" =======");
			for (String s : hashMap.keySet()) {
				
				Object value = hashMap.get(s);
				System.out.println(s + ":" + value.toString());
			}
		}*/
		
		

		// file.addDataByTextFormat(new String [] {"user_name=Abstracto
		// Nagua","id=1","hash=9223000000000000000"});
		// file.addDataByTextFormat(new String [] {"user_name=Hax
		// Pool","id=2","hash=1223000000000000000"});
	}
	public static void readBy (String name){
		int c = 0 ;
		ArrayList<DataRow> objs = file.getData("company_name", name);
		for (HashMap<String, Object> hashMap : objs) {
			c++;
			System.out.print("[row "+c+"]:");
			for (String s : hashMap.keySet()) {
				
				Object value = hashMap.get(s);
				System.out.print(s + ":" + value.toString()+";");
			}
			System.out.println();
		}
	}
	
	public static void readDemandEnergy (){
		int c=0;
		ArrayList<DataRow> objs2 = file.getData("company_name", "Demand Energy, Inc.");
		for (HashMap<String, Object> hashMap : objs2) {
			c++;
			System.out.print("[row "+c+"]:");
       		for (String s : hashMap.keySet()) {				
				Object value = hashMap.get(s);
 				System.out.print(s + ":" + value.toString()+";");
			}
       		System.out.println();
		}
	}
	
	public static void readAllRecorda (){
		int c = 0 ;
		ArrayList<DataRow> objs3 = file.readAll();
		for (HashMap<String, Object> hashMap : objs3) {
			String st = (String)hashMap.get("company_name");
			if (st.compareTo("Demand Energy, Inc.")==0){
				c++;
				System.out.print("[row "+c+"]:");
				for (String s : hashMap.keySet()) {					
					Object value = hashMap.get(s);
					System.out.print(s + ":" + value.toString()+";");
				}
				System.out.println();
			}
		}	
	}
	
	public static void load (){
		try {
			int c =0;
			int cc=0;
			BufferedReader br = new BufferedReader(new FileReader(new File("C:/tmp/data.csv")));
			String line;
			 c = 0;
			while ((line = br.readLine()) != null) {
				c++;
				System.out.println(c);
				String[] pair = line.split(",", 2);
				if (pair.length != 2) {
					pair = new String[] { pair[0], "" };
				}
				if (pair[1].isEmpty()) {
					cc++;
					//System.out.println(cc);
				}
				DataRow map = new DataRow();
				map.put("company_name", pair[1]);
				map.put("id", Integer.parseInt(pair[0]));
				map.put("hash", new Random(System.currentTimeMillis()).nextInt());
				file.addData(map);
				//break;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
