package it.uniroma3.dia.sii.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.KeyValue;

public class HbaseWrapper {
	private HBaseConfiguration hc = new HBaseConfiguration(new Configuration());

	public HbaseWrapper(HBaseConfiguration hc) {
		this.hc = hc;
	}	

	public void addRecord(String tableName, String rowKey, String family, String qualifier, String value) throws Exception{
		HTable ht = new HTable(hc, tableName);
		Put put = new Put(rowKey.getBytes());
		put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());
		ht.put(put);
		ht.close();
		System.out.println("Aggiunti record " + tableName + " " + rowKey + " " + family + ":" + qualifier + " " + value);
	}

	public RowBean getOneRecord(String tableName, String rowKey) throws IOException{
		HTable ht = new HTable(hc, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = ht.get(get);
		RowBean rb = new RowBean(tableName, rowKey); 		
		for(KeyValue kv : rs.raw()){
			rb.addRowContent((new String(kv.getFamily())), (new String(kv.getQualifier())), 
					(new String(kv.getValue())));
		}
		ht.close();
		return rb;
	}

	public void getAllFamilyRecord(String tableName, String rowKey, String family) throws IOException{
		HTable ht = new HTable(hc, tableName);
		Scan scan = new Scan();
		scan.addFamily(family.getBytes());
		ResultScanner scanner = ht.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("Trovate righe: " + result);
		}
		scanner.close();
	}

	public void getAllQualifierRecord(String tableName, String rowKey, String family, String qualifier) throws IOException{
		HTable ht = new HTable(hc, tableName);
		Scan scan = new Scan();
		scan.addColumn(family.getBytes(), qualifier.getBytes());
		ResultScanner scanner = ht.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("Trovate righe: " + result);
		}
		scanner.close();
	}

	public void delRecord(String tableName, String rowKey) throws Exception{
		HTable hTable = new HTable(hc, tableName);
		Delete d = new Delete(rowKey.getBytes());
		hTable.delete(d);
		hTable.close();
		System.out.println("Cancellati record " + tableName + " " + rowKey + " (if it ever existed)");
	}

}
