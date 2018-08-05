package com.av.autopivot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import org.springframework.core.env.Environment;

import com.av.csv.CSVFormat;
import com.qfs.desc.IFieldDescription;
import com.qfs.desc.IOptimizationDescription;
import com.qfs.desc.IOptimizationDescription.Optimization;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.FieldDescription;
import com.qfs.desc.impl.OptimizationDescription;
import com.qfs.desc.impl.StoreDescription;
import com.qfs.platform.IPlatform;
import com.qfs.store.part.IPartitioningDescription;
import com.qfs.store.part.impl.ModuloFunctionDescription;
import com.qfs.store.part.impl.PartitioningDescriptionBuilder;

public class AutoPivotRefDataGenerator {
	
	/** Logger **/
	private static final Logger LOGGER = Logger.getLogger(AutoPivotRefDataGenerator.class.getName());
	
	/**
	 * 
	 * Generate a store description based on the discovery of the input data.
	 * 
	 * @param format
	 * @return store description
	 */
	public IStoreDescription createStoreDescription(String dataStoreName, CSVFormat format, Environment env) {
		List<IFieldDescription> fields = new ArrayList<>();
		List<IOptimizationDescription> optimizations = new ArrayList<>();

		for(int c = 0; c < format.getColumnCount(); c++) {
			String columnName = format.getColumnName(c);
			String columnType = format.getColumnType(c);
			FieldDescription desc = new FieldDescription(columnName, columnType);

			// For date fields automatically add YEAR - MONTH - DAY fields
			if(columnType.startsWith("DATE")) {
				FieldDescription year = new FieldDescription(columnName + ".YEAR", "int");
				optimizations.add(new OptimizationDescription(year.getName(), Optimization.DICTIONARY));
				FieldDescription month = new FieldDescription(columnName + ".MONTH", "string");
				optimizations.add(new OptimizationDescription(month.getName(), Optimization.DICTIONARY));
				FieldDescription day = new FieldDescription(columnName + ".DAY", "int");
				optimizations.add(new OptimizationDescription(day.getName(), Optimization.DICTIONARY));
				
				fields.add(year);
				fields.add(month);
				fields.add(day);
			}

			// Dictionarize objects and integers so they can be used
			// as ActivePivot levels.
			if(columnType.startsWith("DATE")
					|| "int".equalsIgnoreCase(columnType)
					|| "String".equalsIgnoreCase(columnType)) {
				optimizations.add(new OptimizationDescription(columnName, Optimization.DICTIONARY));
			}

			fields.add(desc);
		}

		// Partitioning
		IPartitioningDescription partitioning = createPartitioningDescription(format, env);
		
		@SuppressWarnings("unchecked")
		StoreDescription desc = new StoreDescription(
				dataStoreName,
				Collections.EMPTY_LIST,
				fields,
				"COLUMN",
				partitioning,
				optimizations,
				false);

		return desc;
	}
	
	/**
	 * 
	 * Automatically configure the partitioning of the datastore.
	 * The first non floating point field is used as the partitioning
	 * field, and the number of partitions is half the number
	 * of cores.
	 * 
	 * @param format
	 * @return partitioning description
	 */
	public IPartitioningDescription createPartitioningDescription(CSVFormat format, Environment env) {
		
		int processorCount = IPlatform.CURRENT_PLATFORM.getProcessorCount();
		int partitionCount = processorCount/2;
		if(partitionCount > 1) {

			String partitioningField = env.getProperty("datastore.partitioningField");
			if(partitioningField != null) {
				
				for(int c = 0; c < format.getColumnCount(); c++) {
					String fieldName = format.getColumnName(c);
					if(fieldName.equalsIgnoreCase(partitioningField)) {
						return new PartitioningDescriptionBuilder()
						.addSubPartitioning(fieldName, new ModuloFunctionDescription(partitionCount))
						.build();
					}
				}
				LOGGER.warning("Configured partitioning field '" + partitioningField + "' does not exist in input file format. Default partitioning will be used.");
			}
			
			// Default partitioning, partition on the first field
			// that is not numerical
			for(int c = 0; c < format.getColumnCount(); c++) {
				String fieldName = format.getColumnName(c);
				String fieldType = format.getColumnType(c);
					
				if(!"float".equalsIgnoreCase(fieldType) && !"double".equalsIgnoreCase(fieldType) && !"long".equalsIgnoreCase(fieldType)) {
					LOGGER.info("Applying default partitioning policy: " + partitionCount + " partitions with partitioning field '" + fieldName + "'");
					return new PartitioningDescriptionBuilder()
					.addSubPartitioning(fieldName, new ModuloFunctionDescription(partitionCount))
					.build();
				}
			}
		}
		return null;
	}
}
