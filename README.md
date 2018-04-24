# ToltecDatabase. 
Author: Alexey Bochkov, Novosibirsk, Russia. Contact with me: renaissancesoft [at] gmail [d0t] com

Pure java database.

In the development process - use at your own risk.

The basic idea is to quickly access the data on the index and the separation of indexes and data into different disks (index on ssd, data on hdd).

## Simple usage:

```
ToltecDatabase db = new ToltecDatabase();
```
First - folder for index, second for data. last - database name.
```
db.init("C:/tmp/ToltecIndex", "C:/tmp/ToltecData", "MyDB");
```
Add string columns with index
```
db.addCol("url", TableSchema.TYPE_STRING, true);		
```

Add int columns. TYPE_LONG for long data
```
db.addCol("random_hash", TableSchema.TYPE_INT, false);		
```

Add columns for binary data. byte[]
```
db.addCol("data", TableSchema.TYPE_BYTEARRAY, false);
```

TYPE_FLOAT and TYPE_DOUBLE must work. TYPE_OBJECT not supported.
monotor is http server for browse data.
```
db.monitor(8809);
```

### Now we can add data
```
DataRow row = new DataRow();
row.add("url", "http://google.com");
row.add("random_hash", 1234);
row.add("data", "text data to byte array".getBytes());
```

send write task.
```
db.addData(row);
```

any update working async and not return (yet) info about finish. If write stack will be full, thread will be paused.
no need wait for write but you can:
```
while(db.commitFinished()==false) {
    try {
        Thread.sleep(10);
    } catch (InterruptedException e) {
        e.printStackTrace();
    }
}
```

### Lets read data:
```
ArrayList<DataRow> readedData = db.getData("url", "http://google.com");
```
DataRow entends from HashMap <String,Object> and must be used as hashmap
Object can be String, Integer, Long, byte[], Float, Double
right now supported only exact comparison. 
Reading work in separetad from write and update thread. 

### Lets update data:
Update data in record where url = http://google.com, change url to http://bing.com
```
DataRow rowU = new DataRow();
row.add ("url", "http://bing.com");
db.updateData(rowU, "url", "http://google.com");
```

### Now remove data:
```
db.deleteData("url", "http://bing.com");
```
Database can work without any index but of cource - change or read will be slow.

### Now read all data:
```
db.readData(new DataRow.DataRowReader() {
    int c = 0 ; 
    @Override
    public void read(DataRow row) {				
        String url = row.get("url").toString();
        c++;
        System.out.print(""+c+":"+url);
    }

    @Override
    public void end() {

    }
});
```

### Repair table
if something broke data or index table you can repair table. data from old table will be readed one by one and inserted to new table
```
db.repair();
```

## Multithreaded write
since writing, updating and deleting is possible only in one thread, this may not give the desired speed. If you need more performance - you need to use ToltecRaid
```
ToltecRaid raid = new ToltecRaid()
raid.addInstance(0, "C:/tmp/RaidIndex", "C:/tmp/RaidData", "Raid_0");
raid.addInstance(1, "C:/tmp/RaidIndex", "C:/tmp/RaidData", "Raid_1");
raid.addInstance(2, "C:/tmp/RaidIndex", "C:/tmp/RaidData", "Raid_2");
raid.addInstance(3, "C:/tmp/RaidIndex", "C:/tmp/RaidData", "Raid_3");
raid.addCol("url", TableSchema.TYPE_STRING, true);
raid.addCol("random_hash", TableSchema.TYPE_INT, false);
raid.addCol("data", TableSchema.TYPE_BYTEARRAY, false);
raid.setPrimaryIndex("url");
```

NOTE: raid.setPrimaryIndex("url"); - it's importand. raid can't work without primary index. 
It will distribute the added data between the databases based on the remainder of the hash division of the data.
if hash%4 == 0 - data writed to Raid_0
if hash%4 == 3 - data writed to Raid_4

after that you can work with raid like with database.
for repair:
```
raid.repair(0);
raid.repair(2);
```

repair first and 3th instance.





