package com.av.pivot.analysishierarchy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.qfs.store.IDatastoreVersion;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.record.IRecordReader;
import com.quartetfs.biz.pivot.IActivePivot;
import com.quartetfs.biz.pivot.cube.hierarchy.IAnalysisHierarchyInfo;
import com.quartetfs.biz.pivot.cube.hierarchy.IMultiVersionHierarchy;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.impl.AAnalysisHierarchy;
import com.quartetfs.biz.pivot.query.impl.QueryCache;
import com.quartetfs.fwk.QuartetExtendedPluginValue;

@QuartetExtendedPluginValue(intf = IMultiVersionHierarchy.class, key = CurrencyGroupAnalysisHierarchy.PLUGIN_KEY)
public class CurrencyGroupAnalysisHierarchy extends AAnalysisHierarchy {

	/** serialVersionUID */
	private static final long serialVersionUID = -3133657589316087844L;
	
	/** analysis Hierarchy plugin key */
	public static final String PLUGIN_KEY =  "CUR_GROUP_AH";

	/** data store name used to store group of currencies */
	private static final String CURRENCY_GROUP_STORE_NAME = "currency_group";
	
	/** data store fields which contains group definitions */
	private static final String CURRENCY = "CURRENCY";
	private static final String GROUP = "GROUP";
	
	/** Active Pivot */
	protected final IActivePivot pivot;

	public CurrencyGroupAnalysisHierarchy(IAnalysisHierarchyInfo info, IActivePivot pivot) {
		super(info);
		this.pivot = pivot;
	}

	@Override
	public Collection<Object[]> buildDiscriminatorPaths() {
		return getCurrencyGroupFromStore();
	}

	private Collection<Object[]> getCurrencyGroupFromStore() {
		final IDatastoreVersion dv = getDatastoreVersion();
		final List<Object[]> result = new ArrayList<>();
		
		IDictionaryCursor cursor = dv.getQueryRunner()
									 .forStore(CURRENCY_GROUP_STORE_NAME)
									 .withoutCondition()
									 .selectingAllStoreFields()
									 .run();
		if (cursor.hasNext()) {
			while (cursor.hasNext()) {
				cursor.next();
				
				IRecordReader reader = cursor.getRecord();
				result.add(new Object[] { reader.read(CURRENCY),
										  reader.read(GROUP) });
			}
		}
		return result;
	}
	
	protected IDatastoreVersion getDatastoreVersion() {
		return QueryCache.retrieveDatastoreVersion(getActivePivot().getContext());
	}

	private IActivePivot getActivePivot() {
		return pivot;
	}

	@Override
	public int getLevelsCount() {
		return 2;
	}

	@Override
	public String getType() {
		return PLUGIN_KEY;
	}

}
