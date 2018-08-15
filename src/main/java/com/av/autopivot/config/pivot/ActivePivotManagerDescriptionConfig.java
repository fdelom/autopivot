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
package com.av.autopivot.config.pivot;

import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;

import com.av.autopivot.AutoPivotDiscoveryCreator;
import com.av.autopivot.AutoPivotGenerator;
import com.av.autopivot.StoreInfo;
import com.av.autopivot.config.datastore.DatastoreDescriptionConfig;
import com.av.autopivot.config.properties.AutoPivotProperties;
import com.av.autopivot.config.properties.AutoPivotProperties.DataInfo;
import com.av.autopivot.config.source.SourceConfig;
import com.av.csv.CSVFormat;
import com.av.pivot.aggregation.SumOrStringAggregateFunction;
import com.av.pivot.analysishierarchy.CurrencyGroupAnalysisHierarchy;
import com.av.pivot.analysishierarchy.FxTargetCurrencyAnalysisHierarchy;
import com.av.pivot.postprocessing.CurrencyGroupManyToManyPostProcessor;
import com.av.pivot.postprocessing.FXPostProcessor;
import com.qfs.server.cfg.IActivePivotManagerDescriptionConfig;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.IAggregatedMeasureDescription;
import com.quartetfs.biz.pivot.definitions.IAxisDimensionDescription;
import com.quartetfs.biz.pivot.definitions.IAxisHierarchyDescription;
import com.quartetfs.biz.pivot.definitions.IPostProcessorDescription;
import com.quartetfs.biz.pivot.definitions.impl.AggregatedMeasureDescription;
import com.quartetfs.biz.pivot.definitions.impl.AxisDimensionDescription;
import com.quartetfs.biz.pivot.definitions.impl.AxisHierarchyDescription;
import com.quartetfs.biz.pivot.definitions.impl.PostProcessorDescription;
import com.quartetfs.biz.pivot.postprocessing.impl.ADynamicAggregationPostProcessor;

/**
 * 
 * Configure the ActivePivot Manager for the AutoPivot application.
 * <p>
 * The description of the cube are generated automatically
 * based on the format of the CSV file.
 * 
 * @author ActiveViam
 *
 */
public class ActivePivotManagerDescriptionConfig implements IActivePivotManagerDescriptionConfig {

	/** Autopivot Configuration */
	@Autowired
	protected AutoPivotProperties autoPivotProps;

	/** Datasource configuration */
	@Autowired
	protected SourceConfig sourceConfig;

	/** Datastore configuration */
	@Autowired
	protected DatastoreDescriptionConfig datastoreConfig;
	
	@Autowired
	protected AutoPivotDiscoveryCreator discoveryCreator;

	@Override
	public IActivePivotManagerDescription managerDescription() {

		AutoPivotGenerator generator = datastoreConfig.generator();
		
		Map<String, DataInfo> dataInfoMap = autoPivotProps.getDataInfoMap();
		for (String key : dataInfoMap.keySet()) {
			DataInfo dataInfo = dataInfoMap.get(key);
			
			CSVFormat discovery = discoveryCreator.createDiscoveryFormat(dataInfo);
			
			StoreInfo storeDesc = StoreInfo.createStoreInfo(key, dataInfo, discovery);
			generator.createCube(storeDesc);
			
			if (key.equals("risks")) {
				addCustomDimensions(generator, storeDesc);
				addCustomPostProcessors(generator, storeDesc);
			}
		}
		return generator.getActivePivotManagerDescription();
	}
	
	private void addCustomDimensions(AutoPivotGenerator generator, StoreInfo storeDesc) {
		/////////////////////////////////////////
		// Create Analysis Dimension
		IAxisDimensionDescription analysisDimension = new AxisDimensionDescription("Analysis Dimension");

		Properties props = new Properties();
		props.put("description", "Analysis Dimension associated to custom post processors");
		analysisDimension.setProperties(props);
		
		/////////////////////////////////////////
		// Add analysis hierarchy used to select fx target currency
		IAxisHierarchyDescription fxTargetCurrencyHierarchy = new AxisHierarchyDescription("FxTargetCurrency",
																						   "CustomAH",
																						   false);
		
		// Associate AAnalysis Hierarchy
		fxTargetCurrencyHierarchy.setPluginKey(FxTargetCurrencyAnalysisHierarchy.PLUGIN_KEY);
		
		// Set up description
		props = new Properties();
		props.put("description", "Target currency used to countervaluate pnl");
		fxTargetCurrencyHierarchy.setProperties(props);
		
		/////////////////////////////////////////
		// Add Analysis hierarchy used to select currencies group
		IAxisHierarchyDescription fxGroupCurrencyHierarchy = new AxisHierarchyDescription("CurrencyGroup",
																						  "CustomAH",
																						  true);
		
		// Associate AAnalysis Hierarchy
		fxGroupCurrencyHierarchy.setPluginKey(CurrencyGroupAnalysisHierarchy.PLUGIN_KEY);
		
		// Set up description
		props = new Properties();
		props.put("description", "Handle currencies group name");
		fxGroupCurrencyHierarchy.setProperties(props);
		
		/////////////////////////////////////////
		// Associate Dimension x Hierarchy
		analysisDimension.getHierarchies().add(fxTargetCurrencyHierarchy);
		analysisDimension.getHierarchies().add(fxGroupCurrencyHierarchy);
		generator.getActivePivotDescription(storeDesc.getStoreName())
				 .getAxisDimensions()
				 .getValues()
				 .add(analysisDimension);
	}
	
	private void addCustomPostProcessors(AutoPivotGenerator generator, StoreInfo storeDesc) {
		String storeName = storeDesc.getStoreName();
		
		// SumOrString Aggregated Measure
		IAggregatedMeasureDescription sumOrString = new AggregatedMeasureDescription("pnl", SumOrStringAggregateFunction.PLUGIN_KEY);
		sumOrString.setFolder("CustomPP");
		sumOrString.setFormatter(AutoPivotGenerator.DOUBLE_FORMAT);
		sumOrString.setVisible(false);
		generator.getAggregatedMeasuresDescription(storeName)
				 .add(sumOrString);
		
		// FXPostProcessor
		Properties props = new Properties();
		props.setProperty(ADynamicAggregationPostProcessor.LEAF_LEVELS, "Currency@Currency@Currency,FxTargetCurrency@FxTargetCurrency@Analysis Dimension");
		props.setProperty(ADynamicAggregationPostProcessor.AGGREGATION_FUNCTION, SumOrStringAggregateFunction.PLUGIN_KEY);
		IPostProcessorDescription fxPP = new PostProcessorDescription("FxMeasure", FXPostProcessor.PLUGIN_KEY, props);
		fxPP.setFolder("CustomPP");
		fxPP.setFormatter(AutoPivotGenerator.DOUBLE_FORMAT);
		fxPP.setUnderlyingMeasures("pnl.SUM");
		generator.getPostProcessorsDescription(storeName)
				 .add(fxPP);
		
		// CurrencyGroupManyToManyPostProcessor
		props = new Properties();
		props.setProperty(ADynamicAggregationPostProcessor.ANALYSIS_LEVELS_PROPERTY, "CurrencyGroup@CurrencyGroup@Analysis Dimension,CurrencyGroup_1@CurrencyGroup@Analysis Dimension");
		props.setProperty(ADynamicAggregationPostProcessor.LEAF_LEVELS, "Currency@Currency@Currency,CurrencyGroup_1@CurrencyGroup@Analysis Dimension");
		props.setProperty(ADynamicAggregationPostProcessor.AGGREGATION_FUNCTION, SumOrStringAggregateFunction.PLUGIN_KEY);
		IPostProcessorDescription cGMTMPP = new PostProcessorDescription("CGMTMMeasure", CurrencyGroupManyToManyPostProcessor.PLUGIN_KEY, props);
		cGMTMPP.setFolder("CustomPP");
		cGMTMPP.setFormatter(AutoPivotGenerator.DOUBLE_FORMAT);
		cGMTMPP.setUnderlyingMeasures("pnl.SUM");
		generator.getPostProcessorsDescription(storeName)
				 .add(cGMTMPP);
	}
}
