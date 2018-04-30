package ToltecDatabase;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import Tools.Log;
import Tools.MD5;
import Tools.Pair;

public class IndexManager {
	ArrayList<IndexFile> m_sortedIndexes = new ArrayList<>();
	IndexFileMaped m_mapedIndex = null;
	int sortedFiles = 0;
	String m_fileNameBase;
	String m_baseName ;
	Random rand = new Random(System.currentTimeMillis());
	ExecutorService service = Executors.newFixedThreadPool(20);
	public String getRandomHash (){		
		String l = ""+rand.nextLong();
		l = MD5.generateHash(l);
		return l ;
	}
	
	public IndexManager (String fileName){
		m_fileNameBase = fileName;		
		File f = new File(fileName);
		String baseName = f.getName();
		String [] baseArray = baseName.split("\\.");
		m_baseName = baseArray[0];
		File [] files = f.getParentFile().listFiles();
		String name = f.getName();
		for (File file : files) {
			String oneFileName = file.getName();
			if (oneFileName.startsWith(name)&&oneFileName.endsWith(".map")){
				if (file.length()==0) {
					file.delete();
					continue ;
				}
				IndexFileMaped mapedIndex = new IndexFileMaped();				
				mapedIndex.init(file.getAbsolutePath());
				finalizeIndex(mapedIndex);
				continue ;
			}
			if (oneFileName.startsWith(name)){	
				if (file.length()==0) {
					file.delete();
					continue ;
				}
				IndexFile index = new IndexFile(file.getAbsolutePath(), true);
				sortedFiles++;
				m_sortedIndexes.add(index);
			}
			
		}
		service.shutdown();
		try {
			service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
		  e.printStackTrace();
		}				
		m_mapedIndex = new IndexFileMaped();
		m_mapedIndex.init(m_fileNameBase+"."+getRandomHash()+".map");
	}
	
	
	public IndexFileMaped getIndexFileMaped () {
		return m_mapedIndex;
	}
	
	public ArrayList<IndexFile> getSortedIndexes (){
		return m_sortedIndexes;
	}
	public void rename (String prefix) {
		File f = new File(m_fileNameBase);
		File [] files = f.getParentFile().listFiles();
		String name = f.getName();
		for (File file : files) {
			String oneFileName = file.getName();
			if (oneFileName.startsWith(name)){	
				String newFileName = oneFileName.replace(m_baseName, prefix);
				file.renameTo(new File (f.getParentFile(),newFileName));
			}			
		}
	}
	
	public void finalizeIndex (IndexFileMaped mapedIndex){		
		Runnable subThread = new Runnable() {			
			@Override
			public void run() {
				String hash = getRandomHash();
				Log.message("Sorting "+m_fileNameBase+"."+hash);
				mapedIndex.sort();	
				mapedIndex.close();				
				Log.message("Save index as "+m_fileNameBase+"."+hash);
				File f = new File (mapedIndex.m_fileName);
				f.renameTo(new File (m_fileNameBase+"."+hash));
				IndexFile indexfile = new IndexFile(new File (m_fileNameBase+"."+hash).getAbsolutePath(), true);
				synchronized (m_sortedIndexes) {
					m_sortedIndexes.add(indexfile);			
					sortedFiles++;			
				}
			}
		};
		subThread.run();
	}
	ConcurrentLinkedQueue<Pair<Long,Long>> m_addQueue = new ConcurrentLinkedQueue<Pair<Long,Long>>();
	
	public void add (long hash, long index){
		m_addQueue.offer(new Pair<Long,Long>(hash,index));
	}
	
	public void systemAdd (long hash, long index){
		if (m_mapedIndex.countElements()>100000){
			IndexFileMaped oldMap = m_mapedIndex;
			finalizeIndex(oldMap);
			m_mapedIndex = new IndexFileMaped();
			m_mapedIndex.init(m_fileNameBase+"."+getRandomHash()+".map");
		}
		m_mapedIndex.add(hash, index);
	}
	
	
	public int updateHash (long oldHash, long newHash, long oldPos, long newPos){
		for (IndexFile index : m_sortedIndexes) {
			Pair<long[],long[]> ipairs = index.getPositionAndIndexesByHash(oldHash);	
			if (ipairs.first().length>0){			
				if (oldHash != newHash){
					for (int i = 0 ; i < ipairs.first().length; i++){
						if (ipairs.second()[i]==oldPos) {
							index.cleanHashInPostion(ipairs.first()[i]);
							if (oldPos!=newPos)
								m_mapedIndex.add(newHash, newPos);
							else
								m_mapedIndex.add(newHash, oldPos);
						}
					}
				}else{
					if (newPos!=0){
						for (int i = 0 ; i < ipairs.first().length; i++){
							if (ipairs.second()[i]==oldPos) {
								index.updatePosition (ipairs.first()[i],newPos);
							}
						}
					}
					
				}						
			}
		}
		return m_mapedIndex.updateHash(oldHash, newHash, oldPos, newPos);
	}
	
	public RecordData[] getIndexesByHash (long hash){ // return offsets in record in database		
		ArrayList<Pair<long[],long[]> > listPairs = new ArrayList<>();
		Pair<long[],long[]> pair = m_mapedIndex.getPositionAndIndexesByHash(hash);
		listPairs.add(pair);
		
		for (IndexFile index : m_sortedIndexes) {
			Pair<long[],long[]> ipairs = index.getPositionAndIndexesByHash(hash);	
			listPairs.add(ipairs);
		}
		ArrayList<RecordData> recordDataList = new ArrayList<>(); 	
		for (Pair<long[], long[]> ppair : listPairs) {
			for (int i =0 ; i < ppair.second().length; i++){
				recordDataList.add(new RecordData(hash,ppair.second()[i]));					
			}
		}
		return recordDataList.toArray(new RecordData[recordDataList.size()]);

	}
	
	public int countElements (){
		int count = 0 ;
		for (IndexFile index : m_sortedIndexes) {
			count += index.countElements();
		}
		count += m_mapedIndex.countElements();
		return count;
	}
}
