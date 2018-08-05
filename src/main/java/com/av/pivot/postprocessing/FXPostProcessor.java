package com.av.pivot.postprocessing;

import java.util.Properties;

import com.qfs.condition.ICondition;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.store.IDatastoreVersion;
import com.qfs.store.query.ICursor;
import com.qfs.store.query.IRecordQuery;
import com.qfs.store.query.condition.impl.RecordQuery;
import com.qfs.store.record.IRecordReader;
import com.quartetfs.biz.pivot.ILocation;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IPostProcessorCreationContext;
import com.quartetfs.biz.pivot.impl.LocationUtil;
import com.quartetfs.biz.pivot.postprocessing.IPostProcessor;
import com.quartetfs.biz.pivot.postprocessing.impl.ADynamicAggregationPostProcessor;
import com.quartetfs.biz.pivot.query.IQueryCache;
import com.quartetfs.fwk.QuartetException;
import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.QuartetRuntimeException;

@QuartetExtendedPluginValue(intf = IPostProcessor.class, key = FXPostProcessor.PLUGIN_KEY)
public class FXPostProcessor extends ADynamicAggregationPostProcessor<Object, Object>{

	/** serialVersionUID */
	private static final long serialVersionUID = 8760095997904977181L;
	
	/** post processor plugin key */
	public static final String PLUGIN_KEY = "FX_PP";

	/** post processor target currency for countervaluation */
	private static final String FX_TARGET_CURRENCY = "EUR";
	
	/** data store name used to store exchange rate */
	private static final String FOREX_STORE_NAME = "fxrate";
	
	/** data store field which contains fx rate */
	private static final String FOREX_RATE = "RATE";
	private static final String FOREX_CURRENCY = "FOREIGN_CUR";
	private static final String FOREX_TARGET_CURRENCY = "CUR";
	
	/** currency level info */
	protected ILevelInfo currencyLevelInfo = null;

	public FXPostProcessor(String name, IPostProcessorCreationContext creationContext) {
		super(name, creationContext);
	}
	
	@Override
	public void init(Properties properties) throws QuartetException {
		super.init(properties);
		
		// init required level values
		if (this.leafLevelsInfo.isEmpty()) {
			throw new QuartetRuntimeException("FXPostProcessor need the currency level info to be able to locate currency attached to the underlying measure.");
		}			
		currencyLevelInfo = this.leafLevelsInfo.get(0);
		
		if (underlyingMeasures.length == 0) {
			throw new QuartetRuntimeException("FXPostProcessor need an associated underlying measure to be able to apply fx countervaluation.");
		}
	}

	@Override
	public String getType() {
		return PLUGIN_KEY;
	}

	@Override
	protected Object evaluateLeaf(ILocation leafLocation, Object[] underlyingMeasures) {
		// Retrieve the currency
		final String currency = (String) LocationUtil.getCoordinate(leafLocation, currencyLevelInfo);

		// Retrieve the measure in the native currency
		final double measureNative = (Double) underlyingMeasures[0];
		
		// Nothing to do when the current & target currencies are the same
		if (FX_TARGET_CURRENCY.equals(currency)) {
			return measureNative;
		}
		
		// Retrieve Rate from cache or datastore
		Object result = getRate(currency, FX_TARGET_CURRENCY); 
		if (result instanceof String) {
			return result;
		}
		return (Double)result * measureNative;
	}

	private Object getRate(String currency, String fxTargetCurrency) {
		final IQueryCache queryCache = getContext().get(IQueryCache.class);
		Object result = queryCache.get(currency);
		
		if (result == null) {
			final Object rateRetrieved = getRateFromDataStore(currency, fxTargetCurrency);
			final Object rateCached = queryCache.putIfAbsent(currency, rateRetrieved);
			result = rateCached == null ? rateRetrieved : rateCached;
		}
		return result;
	}

	private Object getRateFromDataStore(String currency, String fxTargetCurrency) {
		final IDatastoreVersion dv = getDatastoreVersion();
		
		IRecordQuery query = new RecordQuery(FOREX_STORE_NAME, 
											 createCondition(currency, fxTargetCurrency), 
											 FOREX_RATE);
		ICursor cursor = dv.execute(query);
		if (cursor.hasNext() == false) {
			return "Exchange rate of [" + currency + "/" + fxTargetCurrency + "] not found.";
		}
		return getFirstRate(cursor);
	}
	
	private ICondition createCondition(String currency, String fxTargetCurrency) {
		return BaseConditions.And(
		       BaseConditions.Equal(FOREX_CURRENCY, currency),
		       BaseConditions.Equal(FOREX_TARGET_CURRENCY, fxTargetCurrency)
		);
	}
	
	private Double getFirstRate(ICursor cursor) {
	    cursor.next();
	    IRecordReader reader = cursor.getRecord();
		return 1.0d / (Double)reader.read(FOREX_RATE);		
	}
}
