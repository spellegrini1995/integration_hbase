package it.uniroma3.dia.sii.hbase;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RowBean implements Serializable { 
	
	private static final long serialVersionUID = 8730598140581859766L;
	
	private String tableName;
	private String rowKey;
	private Map<String, String> rowContent; 
	
	public RowBean() {
		super();
		this.rowContent = new HashMap<String, String>();  
		this.tableName = "";
		this.rowKey = "";
	}

	public RowBean(String tableName, String rowKey, Map<String, String> rowContent) {
		super();
		this.rowContent = rowContent;
		this.tableName = tableName;
		this.rowKey = rowKey;
	}
	
	public RowBean(String tableName, String rowKey) {
		super();
		this.rowContent = new HashMap<String, String>();
		this.tableName = tableName;
		this.rowKey = rowKey;
	}
	
	public String getValue(String family, String qualifier) {
		return rowContent.get(family + ":" + qualifier);
	}

	public Map<String, String> getRowContent() {
		return rowContent;
	}

	public void setRowContent(Map<String, String> row) {
		this.rowContent = row;
	}
	
	public void addRowContent(String family, String qualifier, String value) {
		this.rowContent.put(family+":"+qualifier, value);
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	@Override
	public String toString() {
		return "RowBean [tableName=" + tableName + ", rowKey=" + rowKey + ", rowContent=" + rowContent + "]";
	}
	
}