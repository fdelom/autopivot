package com.av.pivot.aggregation;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.qfs.agg.IAggregationFunction;
import com.qfs.agg.impl.AGenericAggregationFunction;
import com.qfs.store.Types;
import com.quartetfs.fwk.QuartetPluginValue;

/**
 * Custom AggregateFunction used to SUM if numerical values are encountered.
 * If a String value is encountered, it will be return to the top aggregate.
 * Usefull to manage error or return advises to the end users.
 * 
 * @author Florian
 *
 */
@QuartetPluginValue(intf = IAggregationFunction.class)
public class SumOrStringAggregateFunction extends AGenericAggregationFunction<Object, Object> {

	/** serialVersionUID */
	private static final long serialVersionUID = -5582085756662620960L;

	/** Type identifying this post processor */
	public static final String PLUGIN_KEY = "SUMORSTRING";
	
	public SumOrStringAggregateFunction() {
		super(PLUGIN_KEY, Types.TYPE_OBJECT);
	}
	
	public String getType() { return PLUGIN_KEY; }

	@Override
	protected Object aggregate(boolean removal, Object aggregate, Object inputValue) {
		// Decontribution is not handled
		if (removal) return null;
		
		// Aggregate is null
		// Start aggregation by returning inputValue
		if (aggregate == null) {
			return inputValue;
		}
		// InputValue is null
		// So ignore it and keep current aggregate
		else if (inputValue == null) {
			return aggregate;
		}
		// InputValue is a String
		// Keep it and replace the current aggregate
		else if (inputValue instanceof String) {
			return inputValue;
		}
		// Aggregate & inputValue are Numbers
		// Apply sum and return
		else if (aggregate instanceof Number &&
				 inputValue instanceof Number) {
			return sum(aggregate, inputValue);
		}
		// Default behavior is to return current aggregate
		return aggregate;
	}

	/**
	 * Apply the correct Sum based on the real type
	 * 
	 * @param aggregate current sum
	 * @param inputValue input value to be added
	 * @return sum
	 */
	private Object sum(Object aggregate, Object inputValue) {
		Number aggregateNum = (Number)aggregate;
		Number inputNum = (Number)inputValue;
		
		if (inputValue instanceof Double) {
			return aggregateNum.doubleValue() + inputNum.doubleValue();
		}
		else if (inputValue instanceof Float) {
			return aggregateNum.floatValue() + inputNum.floatValue();
		}
		else if (inputValue instanceof Integer) {
			return aggregateNum.intValue() + inputNum.intValue();
		}
		else if (inputValue instanceof Long) {
			return aggregateNum.longValue() + inputNum.longValue();
		}
		else if (inputValue instanceof Short) {
			return aggregateNum.shortValue() + inputNum.shortValue();
		}
		else if (inputValue instanceof Byte) {
			return aggregateNum.byteValue() + inputNum.byteValue();
		}
		else if (inputValue instanceof BigInteger) {
			BigInteger aggregateNumBig = BigInteger.valueOf(aggregateNum.longValue());
			BigInteger inputNumBig = BigInteger.valueOf(inputNum.longValue());
			return aggregateNumBig.add(inputNumBig);
		}
		else {
			BigDecimal aggregateNumBig = new BigDecimal(aggregateNum.toString());
			BigDecimal inputNumBig = new BigDecimal(inputNum.toString());
			return aggregateNumBig.add(inputNumBig);
		}
	}
	
	@Override
	protected Object merge(boolean removal, Object mainAggregate, Object contributedAggregate) {
		return aggregate(removal, mainAggregate, contributedAggregate);
	}

	@Override
	protected Object cloneAggregate(Object aggregate) {
		return aggregate;
	}
}
