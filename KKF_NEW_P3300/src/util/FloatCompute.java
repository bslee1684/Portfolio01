package util;

import java.math.BigDecimal;
import java.math.MathContext;

// 정확한 실수계산을 위한 모듈
public class FloatCompute {
	
	private FloatCompute(){}
	
	// 더하기
	public static double plus(double val_1, double val_2){
		
		double ret = 0.0;
		
		BigDecimal decimal_1 = BigDecimal.valueOf(val_1);
		BigDecimal decimal_2 = BigDecimal.valueOf(val_2);
		
		ret = decimal_1.add(decimal_2).doubleValue();
		
		return ret;
	}
	
	// 빼기
	public static double minus(double val_1, double val_2){
		
		double ret = 0.0;
		
		BigDecimal decimal_1 = BigDecimal.valueOf(val_1);
		BigDecimal decimal_2 = BigDecimal.valueOf(val_2);
		
		ret = decimal_1.subtract(decimal_2).doubleValue();
		
		return ret;
	}
	
	// 곱하기
	public static double multiply(double val_1, double val_2){
		
		double ret = 0.0;
		
		BigDecimal decimal_1 = BigDecimal.valueOf(val_1);
		BigDecimal decimal_2 = BigDecimal.valueOf(val_2);
		
		ret = decimal_1.multiply(decimal_2).doubleValue();
		
		return ret;
	}
	
	// 나누기
	public static double divide(double val_1, double val_2){
		
		double ret = 0.0;
		
		BigDecimal decimal_1 = BigDecimal.valueOf(val_1);
		BigDecimal decimal_2 = BigDecimal.valueOf(val_2);
		
		ret = decimal_1.divide(decimal_2, MathContext.DECIMAL128).doubleValue();
		
		return ret;
	}
	
	// 소수점 자리수포함하여 올림
	public static double round(double target, int point){
		
		double ret = 0.0;
		
		ret = Math.round(multiply(target, Math.pow(10, point)));
		ret = divide(ret, Math.pow(10, point));
		
		return ret;
		
	}
	
	//백분율 얻어오기
	public static double percentage(double target, int x, int point){
		
		double ret = 0.0;
		
		ret = Math.round(multiply(target, Math.pow(10, x)));
		ret = divide(ret, Math.pow(10, point));
		
		return ret;
	}
}
