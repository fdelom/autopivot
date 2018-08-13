package com.av.pivot.analysishierarchy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.qfs.store.IDatastore;
import com.qfs.store.IDatastoreVersion;
import com.qfs.store.IReadableDatastore;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.selection.IContinuousSelection;
import com.qfs.store.selection.ISelectionListener;
import com.qfs.store.selection.impl.Selection;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.quartetfs.biz.pivot.cube.hierarchy.IAnalysisHierarchyInfo;
import com.quartetfs.biz.pivot.cube.hierarchy.IMultiVersionHierarchy;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.impl.AAnalysisHierarchy;
import com.quartetfs.fwk.QuartetExtendedPluginValue;

@QuartetExtendedPluginValue(intf = IMultiVersionHierarchy.class, key = CurrencyGroupAnalysisHierarchy.PLUGIN_KEY)
public class CurrencyGroupAnalysisHierarchy extends AAnalysisHierarchy {
	
	/** Logger **/
	protected static Logger LOGGER = Logger.getLogger(CurrencyGroupAnalysisHierarchy.class.getName());

	/** serialVersionUID */
	private static final long serialVersionUID = -3133657589316087844L;
	
	/** analysis Hierarchy plugin key */
	public static final String PLUGIN_KEY =  "CUR_GROUP_AH";

	/** data store name used to store group of currencies */
	private static final String CURRENCY_GROUP_STORE_NAME = "currency_group";
	
	/** main store which handles facts */
	private static final String MAIN_STORE_NAME = "risks";	
	
	/** data store fields which contains group definitions */
	private static final String CURRENCY = "CURRENCY";
	private static final String GROUP = "GROUP";
	
	/** default member for each level */
	private static final String DEFAULT_MEMBER_CURRENCY = "No currency";
	private static final String DEFAULT_MEMBER_GROUP = "No group defined";
	
	/** As the listener associated to the currency_group store could not trigger an empty
	 *  transaction within the current transaction event. We need to create an empty transaction 
	 *  on the main store in another thread asynchronously.
	 */
	private static final ExecutorService REBUILD_EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
	private AtomicLong previousEpochId = new AtomicLong(-1L);
	
	public CurrencyGroupAnalysisHierarchy(IAnalysisHierarchyInfo info) {
		super(info);
	}
	
	@Override
	public void init() {
		super.init();
		
		registerContinuousSelection();
	}

	@Override
	public Collection<Object[]> buildDiscriminatorPaths() {
		Collection<Object[]> result = getCurrencyGroupFromStore();
		
		// Store is probably not loaded at this step, so put default values
		if (result.isEmpty()) {
			result.add(new Object[] { DEFAULT_MEMBER_GROUP,
									  DEFAULT_MEMBER_CURRENCY });
		}
		return result;
	}

	private Collection<Object[]> getCurrencyGroupFromStore() {
		final IDatastoreVersion dv = getDatastoreVersion();
		final List<Object[]> result = new ArrayList<>();
		
		if (dv != null) {
			IDictionaryCursor cursor = dv.getQueryRunner()
										 .forStore(CURRENCY_GROUP_STORE_NAME)
										 .withoutCondition()
										 .selectingAllStoreFields()
										 .run();
			if (cursor.hasNext()) {
				while (cursor.hasNext()) {
					cursor.next();
					
					IRecordReader reader = cursor.getRecord();
					result.add(new Object[] { reader.read(GROUP),
											  reader.read(CURRENCY) });
				}
			}
		}
		return result;
	}
	
	private void registerContinuousSelection() {
		IDatastore ds = getDatastore();
		if (ds != null) {
			IContinuousSelection continuousSelection = ds.register(new Selection(CURRENCY_GROUP_STORE_NAME, Collections.emptyList()));
			continuousSelection.addListener(new ISelectionListener() {
				@Override
				public void transactionCommitted(IDatastoreVersion version) {
					REBUILD_EXECUTOR_SERVICE.submit(() -> {
					    emptyTransaction();
					});
				}

				private void emptyTransaction() {
					try{
				        ds.getTransactionManager().startTransaction(MAIN_STORE_NAME);
				        ds.getTransactionManager().commitTransaction();
				    } catch (DatastoreTransactionException e) {
				    	LOGGER.severe("Empty transaction failed.");       
				    }
				}
			});
		}
	}
	
	@Override
	public boolean getNeedRebuild() {
		IDatastore ds = getDatastore();
		if (ds != null) {
			Long currentEpochId = ds.getHead().getEpochId();
			
			if (currentEpochId > previousEpochId.get()) {
				previousEpochId.set(currentEpochId);
				return true;
			}
		}
		return false;
	}
	
	protected IDatastoreVersion getDatastoreVersion() {
		IDatastore ds = getDatastore();
		if (ds != null) {
			return ds.getHead();
		}
		return null;
	}
	
	protected IDatastore getDatastore() {
		if (datastore != null) {
			IReadableDatastore readableDatastore = datastore.getDatastore();
			if (readableDatastore instanceof IDatastore) {
				return (IDatastore)readableDatastore;
			}
		}
		return null;
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
