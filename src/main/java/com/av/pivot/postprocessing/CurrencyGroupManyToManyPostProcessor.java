package com.av.pivot.postprocessing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.qfs.condition.ICondition;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.store.IDatastoreVersion;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.quartetfs.biz.pivot.ILocation;
import com.quartetfs.biz.pivot.ILocationExpansionProcedure;
import com.quartetfs.biz.pivot.IPointLocationBuilder;
import com.quartetfs.biz.pivot.IPointLocationReader;
import com.quartetfs.biz.pivot.cellset.IAggregatesLocationResult;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.cube.hierarchy.impl.HierarchiesUtil;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IPostProcessorCreationContext;
import com.quartetfs.biz.pivot.postprocessing.IPostProcessor;
import com.quartetfs.biz.pivot.postprocessing.impl.ADynamicAggregationPostProcessor;
import com.quartetfs.fwk.QuartetException;
import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.QuartetRuntimeException;

@QuartetExtendedPluginValue(intf = IPostProcessor.class, key = CurrencyGroupManyToManyPostProcessor.PLUGIN_KEY)
public class CurrencyGroupManyToManyPostProcessor extends ADynamicAggregationPostProcessor<Object, Object>{

	/** Logger **/
	protected static Logger LOGGER = Logger.getLogger(CurrencyGroupManyToManyPostProcessor.class.getName());
	
	/** serialVersionUID */
	private static final long serialVersionUID = 6298172968882576777L;
	
	/** post processor plugin key */
	public static final String PLUGIN_KEY = "CGMTM_PP";

	/** currency level info from facts */
	private ILevelInfo currencyLevelInfo;
	
	/** group level info from Analysis Hierarchy */
	private ILevelInfo groupLevelInfo;
	
	/** currency level info from Analysis Hierarchy */
	private ILevelInfo currencyGroupLevelInfo;

	public CurrencyGroupManyToManyPostProcessor(String name, IPostProcessorCreationContext creationContext) {
		super(name, creationContext);
	}

	@Override
	public String getType() {
		return PLUGIN_KEY;
	}
	
	@Override
	public void init(Properties properties) throws QuartetException {
		super.init(properties);
		
		// init required level values
		if (this.leafLevelsInfo.isEmpty()) {
			throw new QuartetRuntimeException("CurrencyGroupManyToManyPostProcessor need at least Currency level associated to the facts");
		}			
		currencyLevelInfo = this.leafLevelsInfo.get(0);
		groupLevelInfo = HierarchiesUtil.getLevel(getActivePivot(), "CurrencyGroup").getLevelInfo();
		currencyGroupLevelInfo = HierarchiesUtil.getLevel(getActivePivot(), "CurrencyGroup_1").getLevelInfo();
	}

	@Override
	protected Object evaluateLeaf(ILocation leafLocation, Object[] underlyingMeasures) {
		return underlyingMeasures[0];
	}
	
	@Override
	protected ILocationExpansionProcedure getExpansionProcedure(ILocation queryLocation, ILocation restrictedLocation) {
		return new ILocationExpansionProcedure() {

			@Override
			public IExpansionIterator createIterator(ILocation scope) {
				return new BucketExpansionProcedureIterator();
			}

			@Override
			public List<ILevelInfo> getExpansionLevels() {
				return Arrays.asList(CurrencyGroupManyToManyPostProcessor.this.groupLevelInfo,
									 CurrencyGroupManyToManyPostProcessor.this.currencyGroupLevelInfo);
			}
			
		};
	}
	
	protected class BucketExpansionProcedureIterator implements ILocationExpansionProcedure.IExpansionIterator {

		/** data store name used to store group of currencies */
		private static final String CURRENCY_GROUP_STORE_NAME = "currency_group";
		
		/** data store fields which contains group definitions */
		private static final String CURRENCY = "CURRENCY";
		private static final String GROUP = "GROUP";
		
		/** Current expansion bucket if any */
		protected List<String> groups;
		
		/** Current currency being bucketed */
		protected String currentCurrency;
		
		/** True if the current group level is not set and needs to be */
		protected boolean needBucket;
		
		/** True if the current currency level is not set and needs to be */
		protected boolean needCurrency;
		
		/** True if the expansion procedure should keep on running */
		protected boolean next;
		
		protected List<String> getGroups(String currency) {
			final IDatastoreVersion dv = getDatastoreVersion();
			final List<String> result = new ArrayList<>();
			
			IDictionaryCursor cursor = dv.getQueryRunner()
										 .forStore(CURRENCY_GROUP_STORE_NAME)
										 .withCondition(createConditionGroupsFilterByCurrency(currency))
										 .selecting(GROUP)
										 .run();
			if (cursor.hasNext()) {
				while (cursor.hasNext()) {
					cursor.next();
					
					IRecordReader reader = cursor.getRecord();
					result.add((String)reader.read(GROUP));
				}
			}
			return result;
		}
		
		protected Map<String, List<String>> getGroupsToCurrency() {
			final IDatastoreVersion dv = getDatastoreVersion();
			final Map<String, List<String>> result = new HashMap<>();
			
			IDictionaryCursor cursor = dv.getQueryRunner()
										 .forStore(CURRENCY_GROUP_STORE_NAME)
										 .withoutCondition()
										 .selectingAllStoreFields()
										 .run();
			if (cursor.hasNext()) {
				while (cursor.hasNext()) {
					cursor.next();
					
					IRecordReader reader = cursor.getRecord();
					String group = (String)reader.read(GROUP);
					String currency = (String)reader.read(CURRENCY);
					
					if (result.containsKey(group)) {
						result.get(group).add(currency);
					}
					else {
						List<String> currenciesList = new ArrayList<>();
						currenciesList.add(currency);
						result.put(group, currenciesList);
					}
				}
			}
			return result;
		}
		
		private ICondition createConditionGroupsFilterByCurrency(String currency) {
			return BaseConditions.Equal(CURRENCY, currency);
		}
		
		@Override
		public boolean hasNext() {
			return groups != null &&
				   groups.isEmpty() == false &&
				   next == true;
		}

		@Override
		public void reset(IPointLocationReader location, IAggregatesLocationResult aggregates) {
			// Compute the target group based on the current currency value
			final ILevelInfo bucketedLevel = CurrencyGroupManyToManyPostProcessor.this.currencyLevelInfo;
			final Object currencyMember = location.getCoordinate(bucketedLevel.getHierarchyInfo().getOrdinal() - 1,
																 bucketedLevel.getOrdinal());
			if (IRecordFormat.GLOBAL_DEFAULT_OBJECT.equals(currencyMember)) {
				return ; // N/A are ignored
			}
			else if (!(currencyMember instanceof String)) {
				throw new QuartetRuntimeException("Unexpected member '" + currencyMember + "'");				
			}
			currentCurrency = (String)currencyMember;
			groups = getGroups(currentCurrency);
			if (groups == null) {
				LOGGER.warning("Store used to define groups of currencies is probably empty or currency: " + 
							   currentCurrency + 
							   " has no group");
			}
			// Keep the bucket if it is compatible with the current bucket coordinate
			final ILevelInfo groupLevel = CurrencyGroupManyToManyPostProcessor.this.groupLevelInfo;
			final Object groupMember = location.getCoordinate(groupLevel.getHierarchyInfo().getOrdinal() - 1,
															  groupLevel.getOrdinal());
			if (groupMember != null) {
				// A group coordinate already exist.
				// Keep only or bucket if it matches the current one
				if (getGroupsToCurrency().get(groupMember).contains(currencyMember)) {
					// Set the groups to call at least once setNext
					groups = new ArrayList<>();
					groups.add((String)groupMember);
				}
				else {
					groups = null;
				}
			}
			else {
				// The bucket/group coordinate is not set
				// We need to set it when asked to.
				needBucket = true;
			}
			final ILevelInfo currencyLevel = CurrencyGroupManyToManyPostProcessor.this.currencyGroupLevelInfo;
			final Object currentCurrencyMember = location.getCoordinate(currencyLevel.getHierarchyInfo().getOrdinal() - 1,
																		currencyLevel.getOrdinal());
			if (currentCurrencyMember != null) {
				if (currentCurrencyMember.equals(currentCurrency) == false) {
					needCurrency = false;
				}
				else {
					next = true;
				}
			}
			else {
				// The currency coordinate is not set
				needCurrency = true;
				next = true;
			}
		}

		@Override
		public void setNext(IPointLocationBuilder builder) {
			if (groups != null && groups.isEmpty() == false) {
				// Set the bucket if needed
				if (needBucket == true) {
					String bucket = groups.remove(0);
					final ILevelInfo bucketLevel1 = CurrencyGroupManyToManyPostProcessor.this.groupLevelInfo;
					builder.setCoordinate(bucketLevel1.getHierarchyInfo().getOrdinal() - 1,
										  bucketLevel1.getOrdinal(),
										  bucket);
				}
				else {
					groups.remove(0); // only one bucket
				}
				// Set the currency if needed
				if (needCurrency == true) {
					final ILevelInfo bucketLevel2 = CurrencyGroupManyToManyPostProcessor.this.currencyGroupLevelInfo;
					builder.setCoordinate(bucketLevel2.getHierarchyInfo().getOrdinal() - 1,
										  bucketLevel2.getOrdinal(),
										  currentCurrency);
				}
				else if (groups.isEmpty()) {
					next = false;
				}
			}
		}
		
	}
}
