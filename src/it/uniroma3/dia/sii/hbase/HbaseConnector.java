package it.uniroma3.dia.sii.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseConnector {
	public static void main(String[] argv) throws Exception{
		//stringhe di config
		String tableName= "dia";
		String[] family = {"informatica", "automazione"};
		
		//inizializzo la configurazione
		HBaseConfiguration hc = new HBaseConfiguration(new Configuration());

		//inizializzo la tabella
		HTableDescriptor ht = new HTableDescriptor(tableName);

		//inizializzo le family
		ht.addFamily(new HColumnDescriptor(family[0]));
		ht.addFamily(new HColumnDescriptor(family[1]));

		System.out.println("Connessione...");

		//creo la tabella
		HBaseAdmin hba = new HBaseAdmin(hc);
		System.out.println("Creo la tabella...");
		hba.createTable(ht);
		System.out.println("Fatto!");

		//inizio a popolare la tabella
		HbaseWrapper hbw = new HbaseWrapper(hc);
		hbw.addRecord(tableName, "riga1", family[0], "BD", "Simone");
		hbw.addRecord(tableName, "riga2", family[0], "IA", "Francesco");
		hbw.addRecord(tableName, "riga3", family[1], "RO", "Antonella");
		hbw.addRecord(tableName, "riga4", family[1], "Automatica", "Carlo");
		hbw.addRecord(tableName, "riga5", family[0], "BD", "Biagio");
		hbw.addRecord(tableName, "riga6", family[0], "BD", "Maurizio");
		hbw.addRecord(tableName, "riga7", family[1], "RO", "Nicholas");
		hbw.addRecord(tableName, "riga8", family[1], "Automatica", "Vins");
		hbw.addRecord(tableName, "riga9", family[0], "IA", "Luca");
		hbw.addRecord(tableName, "riga10", family[0], "Reti", "Marco");
		
		//query
		//tutte le persone in informatica

		System.out.println("Tutte le persone di informatica");
		hbw.getAllFamilyRecord(tableName, family[0]);	
		
		//tutte le persone in automazione
		System.out.println("Tutte le persone di automazione");
		hbw.getAllFamilyRecord(tableName, family[1]);

		//tutte le persone in informatica nel gruppo di basi di dati
		System.out.println("Tutte le persone del gruppo di ricerca BD");
		hbw.getAllQualifierRecord(tableName, family[0], "BD");	

		//tutte le persone in automazione del gruppo di ricerca operativa
		System.out.println("Tutte le persone del gruppo di ricerca RO");
		hbw.getAllQualifierRecord(tableName, family[1], "RO");	

	}
}
