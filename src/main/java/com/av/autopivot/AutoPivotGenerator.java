/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.av.autopivot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

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
import com.qfs.store.selection.ISelectionField;
import com.qfs.store.selection.impl.SelectionField;
import com.qfs.util.impl.QfsArrays;
import com.quartetfs.biz.pivot.cube.dimension.IDimension.DimensionType;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo.LevelType;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IMeasureHierarchy;
import com.quartetfs.biz.pivot.definitions.IActivePivotDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotSchemaDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotSchemaInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IAggregateProviderDefinition;
import com.quartetfs.biz.pivot.definitions.IAggregatedMeasureDescription;
import com.quartetfs.biz.pivot.definitions.IAggregatesCacheDescription;
import com.quartetfs.biz.pivot.definitions.IAxisDimensionDescription;
import com.quartetfs.biz.pivot.definitions.IAxisHierarchyDescription;
import com.quartetfs.biz.pivot.definitions.IAxisLevelDescription;
import com.quartetfs.biz.pivot.definitions.ICatalogDescription;
import com.quartetfs.biz.pivot.definitions.IMeasuresDescription;
import com.quartetfs.biz.pivot.definitions.INativeMeasureDescription;
import com.quartetfs.biz.pivot.definitions.IPostProcessorDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotSchemaDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotSchemaInstanceDescription;
import com.quartetfs.biz.pivot.definitions.impl.AggregateProviderDefinition;
import com.quartetfs.biz.pivot.definitions.impl.AggregatedMeasureDescription;
import com.quartetfs.biz.pivot.definitions.impl.AggregatesCacheDescription;
import com.quartetfs.biz.pivot.definitions.impl.AxisDimensionDescription;
import com.quartetfs.biz.pivot.definitions.impl.AxisDimensionsDescription;
import com.quartetfs.biz.pivot.definitions.impl.AxisHierarchyDescription;
import com.quartetfs.biz.pivot.definitions.impl.AxisLevelDescription;
import com.quartetfs.biz.pivot.definitions.impl.CatalogDescription;
import com.quartetfs.biz.pivot.definitions.impl.MeasuresDescription;
import com.quartetfs.biz.pivot.definitions.impl.NativeMeasureDescription;
import com.quartetfs.biz.pivot.definitions.impl.PostProcessorDescription;
import com.quartetfs.biz.pivot.definitions.impl.SelectionDescription;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;

/**
 * 
 * Describe the components of an ActivePivot application
 * automatically based on the input data format.
 * 
 */
public class AutoPivotGenerator {
	/** Logger **/
	protected static final Logger LOGGER = Logger.getLogger(AutoPivotGenerator.class.getName());
	
	/** Reference Packages */
	private final static String ACTIVEVIAM_PACKAGE = "com.activeviam";
	private final static String QUARTETFS_PACKAGE = "com.quartetfs";
	private final static String QFS_PACKAGE = "com.qfs";
	
	/** Default format for double measures */
	public static final String DOUBLE_FORMAT = "DOUBLE[#,###.00;-#,###.00]";
	
	/** Default format for integer measures */
	public static final String INTEGER_FORMAT = "INT[#,###;-#,###]";
	
	/** Default format for date levels */
	public static final String DATE_FORMAT = "DATE[yyyy-MM-dd]";
	
	/** Default format for time levels */
	public static final String TIME_FORMAT = "DATE[HH:mm:ss]";	
	
	private static final Set<String> NUMERICS = QfsArrays.mutableSet("double", "float", "int", "long");
	private static final Set<String> INTEGERS = QfsArrays.mutableSet("int", "long");
	private static final Set<String> DECIMALS = QfsArrays.mutableSet("double", "float");
	private static final Set<String> NUMERICS_ONLY = QfsArrays.mutableSet("double", "float", "long");
	
	/** Active Pivot Manager descriptions */
	private volatile IActivePivotManagerDescription activePivotManagerDescription = null;
	
	/** Active Pivot descriptions Map */
	private volatile Map<String, IActivePivotDescription> activePivotDescriptionMap = null;
		
	public static void initRegistry(List<String> packageList) {
		List<String> consolidatedPackageList = new ArrayList<>();
		
		consolidatedPackageList.add(ACTIVEVIAM_PACKAGE);
		consolidatedPackageList.add(QUARTETFS_PACKAGE);
		consolidatedPackageList.add(QFS_PACKAGE);
		
		consolidatedPackageList.addAll(packageList);
		
		String[] packageArray = consolidatedPackageList.toArray(new String[0]);
		Registry.setContributionProvider(new ClasspathContributionProvider(packageArray));
	}
	
	/**
	 * 
	 * Generate a complete ActivePivot Manager description, with one new catalog,
	 * one new schema and one new cube, based on the provided input data format.
	 * 
	 * @param storeDesc input data format
	 */
	public void createCube(StoreInfo storeDesc) {
		ICatalogDescription catalog = new CatalogDescription(storeDesc.getStoreName() + "_CATALOG", Arrays.asList(storeDesc.getStoreName()));
		IActivePivotSchemaDescription schema = createActivePivotSchemaDescription(storeDesc);
		IActivePivotSchemaInstanceDescription instance = new ActivePivotSchemaInstanceDescription(storeDesc.getStoreName() + "_SCHEMA", schema);
		
		getCatalogs().add(catalog);
		getSchemas().add(instance);
	}
	
	private IActivePivotSchemaDescription createActivePivotSchemaDescription(StoreInfo storeDesc) {
		ActivePivotSchemaDescription desc = new ActivePivotSchemaDescription();

		// Datastore selection
		List<ISelectionField> fields = new ArrayList<>();
		for(int f = 0; f < storeDesc.getColumnCount(); f++) {
			String fieldName = storeDesc.getColumnName(f);
			String fieldType = storeDesc.getColumnType(f);
			fields.add(new SelectionField(fieldName));
			
			if(fieldType.startsWith("DATE")) {
				fields.add(new SelectionField(fieldName + ".YEAR"));
				fields.add(new SelectionField(fieldName + ".MONTH"));
				fields.add(new SelectionField(fieldName + ".DAY"));
			}
		}
		SelectionDescription selection = new SelectionDescription(storeDesc.getStoreName(), fields);
		
		// ActivePivot instance
		IActivePivotDescription pivot = createActivePivotDescription(storeDesc);
		IActivePivotInstanceDescription instance = new ActivePivotInstanceDescription(storeDesc.getStoreName(), pivot);
		
		desc.setDatastoreSelection(selection);
		desc.setActivePivotInstanceDescriptions(Collections.singletonList(instance));
		
		return desc;
	}
	
	/**
	 * 
	 * Generate a store description based on the discovery of the input data.
	 * 
	 * @param storeDesc input data format
	 * @return store description
	 */
	public IStoreDescription createStoreDescription(StoreInfo storeDesc) {
		List<IFieldDescription> fields = new ArrayList<>();
		List<IOptimizationDescription> optimizations = new ArrayList<>();
		
		for(int c = 0; c < storeDesc.getColumnCount(); c++) {
			String columnName = storeDesc.getColumnName(c);
			String columnType = storeDesc.getColumnType(c);
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
		IPartitioningDescription partitioning = createPartitioningDescription(storeDesc);
		
		@SuppressWarnings("unchecked")
		StoreDescription desc = new StoreDescription(storeDesc.getStoreName(),
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
	 * @param storeDesc input data format
	 * @return partitioning description
	 */
	public IPartitioningDescription createPartitioningDescription(StoreInfo storeDesc) {
		int processorCount = IPlatform.CURRENT_PLATFORM.getProcessorCount();
		int partitionCount = processorCount/2;
		
		if(partitionCount > 1) {
			if (storeDesc.hasPartitionField()) {	
				String partitioningField = storeDesc.getPartitionField();
				for(int c = 0; c < storeDesc.getColumnCount(); c++) {
					String fieldName = storeDesc.getColumnName(c);
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
			for(int c = 0; c < storeDesc.getColumnCount(); c++) {
				String fieldName = storeDesc.getColumnName(c);
				String fieldType = storeDesc.getColumnType(c);
					
				if(!"float".equalsIgnoreCase(fieldType) && 
				   !"double".equalsIgnoreCase(fieldType) && 
				   !"long".equalsIgnoreCase(fieldType)) {
					LOGGER.info("Applying default partitioning policy: " + partitionCount + " partitions with partitioning field '" + fieldName + "'");
					return new PartitioningDescriptionBuilder()
											.addSubPartitioning(fieldName, new ModuloFunctionDescription(partitionCount))
											.build();
				}
			}
			
		}
		return null;
	}
	
	/**
	 * 
	 * Create the description of an ActivePivot cube automatically,
	 * based on the description of the input dataset.
	 * 
	 * @param storeDesc input data format
	 * @return AcivePivot description
	 */
	public IActivePivotDescription createActivePivotDescription(StoreInfo storeDesc) {
		
		IActivePivotDescription activePivotDescription = getActivePivotDescription(storeDesc.getStoreName());
		
		IAggregateProviderDefinition apd = new AggregateProviderDefinition(storeDesc.getAggregateProviderType().name());
		activePivotDescription.setAggregateProvider(apd);
		
		// Hierarchies and dimensions
		createHierarchiesAndDimensions(storeDesc);
		
		// Measures
		createMeasures(storeDesc);
		
		// Native measures
		createNativeMeasures(storeDesc.getStoreName());

		// Add distinct count calculation for each level field
		addDistinctCountPP(storeDesc);

		// Aggregate cache configuration
		initActivePivotCache(storeDesc.getStoreName(), storeDesc.getCacheSize());
		
		return activePivotDescription;
	}

	/**
	 * Set up Active Pivot Aggregate cache with the provided size. If size is null, nothing is done.
	 * 
	 * @param pivotCacheSize cache size
	 */
	private void initActivePivotCache(String storeName, Integer pivotCacheSize) {
		if(pivotCacheSize != null) {
			LOGGER.info("Configuring aggregate cache of size " + pivotCacheSize);
			IAggregatesCacheDescription cacheDescription = new AggregatesCacheDescription();
			cacheDescription.setSize(pivotCacheSize);
			getActivePivotDescription(storeName).setAggregatesCacheDescription(cacheDescription);
		}
	}

	/**
	 * Add a distinct count post processor to all numerics measures
	 * 
	 * @param storeDesc input data format
	 */
	private void addDistinctCountPP(StoreInfo storeDesc) {
		for(int f = 0; f < storeDesc.getColumnCount(); f++) {
			String fieldName = storeDesc.getColumnName(f);
			String fieldType = storeDesc.getColumnType(f);

			if(!NUMERICS_ONLY.contains(fieldType)) {
				IPostProcessorDescription dc = new PostProcessorDescription(fieldName + ".COUNT", "LEAF_COUNT", new Properties());
				String leafExpression = fieldName + "@" + fieldName;
				dc.getProperties().setProperty("leafLevels", leafExpression);
				dc.setFolder("Distinct Count");
				getPostProcessorsDescription(storeDesc.getStoreName()).add(dc);
			}
		}
	}

	/**
	 * Create native measures to the cube ([measureName].Count & [measureName].TIMESTAMP)
	 */
	private void createNativeMeasures(String storeName) {
		// Configure "count" native measure
		INativeMeasureDescription countMeasure = new NativeMeasureDescription(IMeasureHierarchy.COUNT_ID, "Count");
		countMeasure.setFormatter(INTEGER_FORMAT);
		
		// Hide the last update measure that does not work Just In Time
		INativeMeasureDescription lastUpdateMeasure = new NativeMeasureDescription(IMeasureHierarchy.TIMESTAMP_ID);
		lastUpdateMeasure.setVisible(false);
		
		getNativeMeasureDescription(storeName).add(countMeasure);
		getNativeMeasureDescription(storeName).add(lastUpdateMeasure);		
	}

	/**
	 * Create measures based on StoreInfo provided.
	 * 
	 * @param storeDesc input data format
	 */
	private void createMeasures(StoreInfo storeDesc) {
		for(int f = 0; f < storeDesc.getColumnCount(); f++) {
			String fieldName = storeDesc.getColumnName(f);
			String fieldType = storeDesc.getColumnType(f);
			if(NUMERICS.contains(fieldType) && 
			   !fieldName.endsWith("id") && 
			   !fieldName.endsWith("ID")) {
				String storeName = storeDesc.getStoreName();
				addSumMeasure(storeName, fieldName, fieldType);
				addMinMeasure(storeName, fieldName, fieldType);
				addMaxMeasure(storeName, fieldName, fieldType);
				addAvgMeasure(storeName, fieldName, fieldType);
				addStdAndSqrtMeasure(storeName, fieldName, fieldType);
			}
		}
	}

	/**
	 * Add standard deviation and square measures to the cube description only if the measure type 
	 * provided is a decimals
	 * 
	 * @param measureName to be added to the cube ([measureName].std & [measureName].SQ_SUM)
	 * @param measureType type of the measure
	 */
	private void addStdAndSqrtMeasure(String storeName, String measureName, String measureType) {
		if (DECIMALS.contains(measureType)) {
			IAggregatedMeasureDescription sq_sum = new AggregatedMeasureDescription(measureName, "SQ_SUM");
			sq_sum.setVisible(false);
			
			// Shared formula expressions
			String countExpression = "aggregatedValue[contributors.COUNT]";
			String squareSumExpression = "aggregatedValue[" + measureName + ".SQ_SUM]";
			String avgExpression = "aggregatedValue[" + measureName + ".avg]";
			
			// Define a formula post processor to compute the standard deviation
			IPostProcessorDescription std = new PostProcessorDescription(measureName + ".STD", "FORMULA", new Properties());
			String stdFormula = "(" + squareSumExpression + ", " + countExpression + ", /)";
			stdFormula += ", (" + avgExpression + ", " + avgExpression + ", *), -, SQRT";
			std.getProperties().setProperty("formula", stdFormula);

			// Put the measures for that field in one folder
			sq_sum.setFolder(measureName);
			std.setFolder(measureName);
			
			// Setup measure formatters
			sq_sum.setFormatter(INTEGERS.contains(measureType) ? INTEGER_FORMAT : DOUBLE_FORMAT);
			std.setFormatter(DOUBLE_FORMAT);
			
			// Add standard deviation only for floating point inputs
			getAggregatedMeasuresDescription(storeName).add(sq_sum);
			getPostProcessorsDescription(storeName).add(std);
		}
	}

	/**
	 * Add average measure to the cube description only if the measure type provided is a numerics
	 * 
	 * @param measureName to be added to the cube ([measureName].avg)
	 * @param measureType type of the measure
	 */
	private void addAvgMeasure(String storeName, String measureName, String measureType) {
		if(NUMERICS.contains(measureType)) {
			// Shared formula expressions
			String countExpression = "aggregatedValue[contributors.COUNT]";
			String sumExpression = "aggregatedValue[" + measureName + ".SUM]";
			
			// Define a formula post processor to compute the average
			PostProcessorDescription avg = new PostProcessorDescription(measureName + ".avg", "FORMULA", new Properties());
			String formula = sumExpression + ", " + countExpression + ", /";
			avg.getProperties().setProperty("formula", formula);
			
			avg.setFolder(measureName);
			avg.setFormatter(DOUBLE_FORMAT);
			
			getPostProcessorsDescription(storeName).add(avg);
		}
	}

	/**
	 * Add max measure to the cube description only if the measure type provided is a numerics
	 * 
	 * @param measureName to be added to the cube ([measureName].MAX)
	 * @param measureType type of the measure
	 */
	private void addMaxMeasure(String storeName, String measureName, String measureType) {
		if(NUMERICS.contains(measureType)) {
			IAggregatedMeasureDescription max = new AggregatedMeasureDescription(measureName, "max");
			
			max.setFolder(measureName);
			max.setFormatter(INTEGERS.contains(measureType) ? INTEGER_FORMAT : DOUBLE_FORMAT);
			
			getAggregatedMeasuresDescription(storeName).add(max);
		}
	}

	/**
	 * Add min measure to the cube description only if the measure type provided is a numerics
	 * 
	 * @param measureName to be added to the cube ([measureName].MIN)
	 * @param measureType type of the measure
	 */
	private void addMinMeasure(String storeName, String measureName, String measureType) {
		if(NUMERICS.contains(measureType)) {
			IAggregatedMeasureDescription min = new AggregatedMeasureDescription(measureName, "min");
			
			min.setFolder(measureName);
			min.setFormatter(INTEGERS.contains(measureType) ? INTEGER_FORMAT : DOUBLE_FORMAT);
			
			getAggregatedMeasuresDescription(storeName).add(min);
		}
	}

	/**
	 * Add sum measure to the cube description only if the measure type provided is a numerics
	 * 
	 * @param measureName to be added to the cube ([measureName].SUM)
	 * @param measureType type of the measure
	 */
	private void addSumMeasure(String storeName, String measureName, String measureType) {
		if(NUMERICS.contains(measureType)) {
			IAggregatedMeasureDescription sum = new AggregatedMeasureDescription(measureName, "SUM");
			
			sum.setFolder(measureName);
			sum.setFormatter(INTEGERS.contains(measureType) ? INTEGER_FORMAT : DOUBLE_FORMAT);
			
			getAggregatedMeasuresDescription(storeName).add(sum);
		}
	}

	/**
	 * Create Hierarchies and Dimension based on StoreInfo provided
	 * 
	 * @param storeDesc input data format
	 */
	private void createHierarchiesAndDimensions(StoreInfo storeDesc) {
		AxisDimensionsDescription dimensions = new AxisDimensionsDescription();

		for(int f = 0; f < storeDesc.getColumnCount(); f++) {
			String fieldName = storeDesc.getColumnName(f);
			String fieldType = storeDesc.getColumnType(f);

			if(!NUMERICS_ONLY.contains(fieldType)) {
				IAxisDimensionDescription dimension = new AxisDimensionDescription(fieldName);
				IAxisHierarchyDescription h = new AxisHierarchyDescription(fieldName);
				IAxisLevelDescription l = new AxisLevelDescription(fieldName);
				h.getLevels().add(l);
				dimension.getHierarchies().add(h);
				dimensions.addValues(Arrays.asList(dimension));
				
				// For date fields generate the YEAR-MONTH-DAY hierarchy
				if(fieldType.startsWith("DATE")) {
					dimension.setDimensionType(DimensionType.TIME);
					
					List<IAxisHierarchyDescription> hierarchies = new ArrayList<>();
					IAxisHierarchyDescription hierarchy = new AxisHierarchyDescription(fieldName);
					hierarchy.setDefaultHierarchy(true);
					IAxisLevelDescription dateLevel = new AxisLevelDescription(fieldName);
					dateLevel.setFormatter(DATE_FORMAT);
					dateLevel.setLevelType(LevelType.TIME);
					hierarchy.setLevels(Arrays.asList(dateLevel));
					hierarchies.add(hierarchy);
					
					IAxisHierarchyDescription ymd = new AxisHierarchyDescription(fieldName + "_YMD");
					List<IAxisLevelDescription> levels = new ArrayList<>();
					levels.add(new AxisLevelDescription("Year", fieldName + ".YEAR"));
					levels.add(new AxisLevelDescription("Month", fieldName + ".MONTH"));
					levels.add(new AxisLevelDescription("Day", fieldName + ".DAY"));
					ymd.setLevels(levels);
					hierarchies.add(ymd);
					
					dimension.setHierarchies(hierarchies);
				}
			}
		}
		getActivePivotDescription(storeDesc.getStoreName()).setAxisDimensions(dimensions);
	}

	public IActivePivotDescription getActivePivotDescription(String storeName) {
		if (activePivotDescriptionMap == null) {
			synchronized (this) {
				if (activePivotDescriptionMap == null) {
					ConcurrentHashMap<String, IActivePivotDescription> localMap = new ConcurrentHashMap<>();
					localMap.put(storeName, new ActivePivotDescription());
					activePivotDescriptionMap = localMap;
				}
			}
		}
		else {
			if (activePivotDescriptionMap.containsKey(storeName) == false) {
				activePivotDescriptionMap.put(storeName, new ActivePivotDescription());
			}
		}
		return activePivotDescriptionMap.get(storeName);
	}
	
	public List<IAggregatedMeasureDescription> getAggregatedMeasuresDescription(String storeName) {
		IMeasuresDescription measureDesc = getMeasuresDescription(storeName);
		if (measureDesc.getAggregatedMeasuresDescription() == null) {
			synchronized (this) {
				if (measureDesc.getAggregatedMeasuresDescription() == null) {
					measureDesc.setAggregatedMeasuresDescription(new ArrayList<IAggregatedMeasureDescription>());
				}
			}
		}
		return measureDesc.getAggregatedMeasuresDescription();
	}
	
	public List<IPostProcessorDescription> getPostProcessorsDescription(String storeName) {
		IMeasuresDescription measureDesc = getMeasuresDescription(storeName);
		if (measureDesc.getPostProcessorsDescription() == null) {
			synchronized (this) {
				if (measureDesc.getPostProcessorsDescription() == null) {
					measureDesc.setPostProcessorsDescription(new ArrayList<IPostProcessorDescription>());
				}
			}
		}
		return measureDesc.getPostProcessorsDescription();
	}
	
	public IMeasuresDescription getMeasuresDescription(String storeName) {
		IActivePivotDescription activePivotDescription = getActivePivotDescription(storeName);
		if (activePivotDescription.getMeasuresDescription() == null) {
			synchronized (this) {
				if (activePivotDescription.getMeasuresDescription() == null) {
					activePivotDescription.setMeasuresDescription(new MeasuresDescription());
				}
			}
		}
		return activePivotDescription.getMeasuresDescription();
	}
	
	public List<INativeMeasureDescription> getNativeMeasureDescription(String storeName) {
		IMeasuresDescription measureDesc = getMeasuresDescription(storeName);
		if (measureDesc.getNativeMeasures() == null) {
			synchronized (this) {
				if (measureDesc.getNativeMeasures() == null) {
					measureDesc.setNativeMeasures(new ArrayList<INativeMeasureDescription>());
				}
			}
		}
		return measureDesc.getNativeMeasures();
	}
	
	public IActivePivotManagerDescription getActivePivotManagerDescription() {
		if (activePivotManagerDescription == null) {
			synchronized (this) {
				if (activePivotManagerDescription == null) {
					activePivotManagerDescription = new ActivePivotManagerDescription();
				}
			}
		}
		return activePivotManagerDescription;
	}
	
	@SuppressWarnings("unchecked")
	public List<ICatalogDescription> getCatalogs() {
		IActivePivotManagerDescription apManagerDesc = getActivePivotManagerDescription();
		if (apManagerDesc.getCatalogs() == null) {
			synchronized (this) {
				if (apManagerDesc.getCatalogs() == null) {
					apManagerDesc.setCatalogs(new ArrayList<>());
				}
			}
		}
		return (List<ICatalogDescription>) apManagerDesc.getCatalogs();
	}
	
	@SuppressWarnings("unchecked")
	public List<IActivePivotSchemaInstanceDescription> getSchemas() {
		IActivePivotManagerDescription apManagerDesc = getActivePivotManagerDescription();
		if (apManagerDesc.getSchemas() == null) {
			synchronized (this) {
				if (apManagerDesc.getSchemas() == null) {
					apManagerDesc.setSchemas(new ArrayList<>());
				}
			}
		}
		return (List<IActivePivotSchemaInstanceDescription>) apManagerDesc.getSchemas();
	}
}
