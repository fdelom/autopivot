package com.av.autopivot;

import java.util.ArrayList;
import java.util.List;

import com.av.autopivot.config.properties.AutoPivotProperties.APropertyInfo;
import com.av.csv.CSVFormat;
import com.google.common.base.Strings;
import com.quartetfs.fwk.QuartetRuntimeException;

public class StoreInfo {
	/** Column names */
	protected final List<String> columnNames;
	
	/** Column types */
	protected final List<String> columnTypes;
	
	/** Store name */
	protected String storeName;
	
	/** PartitionField */
	protected String partitionField;
	
	/** Active Pivot cache size */
	protected Integer cacheSize;
	
	public StoreInfo(String storeName,
					 String partitionField,
					 List<String> columnNames,
					 List<String> columnTypes,
					 Integer cacheSize) {
		this.storeName = storeName;
		this.partitionField = partitionField;
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.cacheSize = cacheSize;
	}
	
	public StoreInfo(String storeName) {
		this.storeName = storeName;
		this.partitionField = "";
		this.columnNames = new ArrayList<String>();
		this.columnTypes = new ArrayList<String>();
		this.cacheSize = null;
	}

	public void addColumn(String name, String type) {
		if (Strings.isNullOrEmpty(name) ||
			Strings.isNullOrEmpty(type)) {
			throw new QuartetRuntimeException("StoreInfo not correctly defined: name[{}] or type[{}] is null or empty");
		}
		columnNames.add(name);
		columnTypes.add(type);
	}
	
	public String getStoreName() {
		return storeName;
	}
	
	public int getColumnCount() {
		return columnNames.size();
	}
	
	public String getColumnName(int index) {
		return columnNames.get(index);
	}
	
	public String getColumnType(int index) {
		return columnTypes.get(index);
	}
	
	public void setPartitionField(String fieldName) {
		this.setPartitionField(fieldName);
	}
	
	public String getPartitionField() {
		return partitionField;
	}
	
	public boolean hasPartitionField() {
		return Strings.isNullOrEmpty(partitionField) == false;
	}
	
	public Integer getCacheSize() {
		return cacheSize;
	}

	public static StoreInfo createStoreInfo(String storeName, APropertyInfo dataInfo, CSVFormat discovery) {
		StoreInfo storeInfo = new StoreInfo(storeName,
											dataInfo.getDataStorePartitionField(), 
											discovery.getColumnNames(),
											discovery.getColumnTypes(),
											dataInfo.getPivotCacheSize());

		return storeInfo;
	}
}
