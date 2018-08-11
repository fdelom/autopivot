package com.av.pivot.analysishierarchy;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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
	public static final List<String> FX_TARGET_CURRENCIES = Arrays.asList("EUR", "USD");
	
	public FxTargetCurrencyAnalysisHierarchy(IAnalysisHierarchyInfo info) {
		super(info);
	}

	@Override
	public Collection<Object[]> buildDiscriminatorPaths() {
		return FX_TARGET_CURRENCIES.stream()
								   .map(cur -> new Object[] { cur })
								   .collect(Collectors.toList());
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
