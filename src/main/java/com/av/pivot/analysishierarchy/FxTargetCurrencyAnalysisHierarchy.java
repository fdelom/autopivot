package com.av.pivot.analysishierarchy;

import java.util.Collection;

import com.activeviam.collections.ImmutableList;
import com.activeviam.collections.impl.ImmutableArrayList;
import com.quartetfs.biz.pivot.cube.hierarchy.IAnalysisHierarchyInfo;
import com.quartetfs.biz.pivot.cube.hierarchy.IMultiVersionHierarchy;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.impl.AAnalysisHierarchy;
import com.quartetfs.fwk.QuartetExtendedPluginValue;

@QuartetExtendedPluginValue(intf = IMultiVersionHierarchy.class, key = FxTargetCurrencyAnalysisHierarchy.PLUGIN_KEY)
public class FxTargetCurrencyAnalysisHierarchy extends AAnalysisHierarchy {

	/** serialVersionUID */
	private static final long serialVersionUID = 9188440303796557722L;
	
	/** analysis Hierarchy plugin key */
	public static final String PLUGIN_KEY =  "FX_AH";

	/** static members values */
	private static final ImmutableList<Object[]> FX_TARGET_CURRENCIES 
					= ImmutableArrayList.of(new Object[] { "EUR" }, 
											new Object[] { "USD" });
	
	public FxTargetCurrencyAnalysisHierarchy(IAnalysisHierarchyInfo info) {
		super(info);
	}

	@Override
	public Collection<Object[]> buildDiscriminatorPaths() {
		return FX_TARGET_CURRENCIES.toCollection();
	}

	@Override
	public int getLevelsCount() {
		return 1;
	}

	@Override
	public String getType() {
		return PLUGIN_KEY;
	}

}
