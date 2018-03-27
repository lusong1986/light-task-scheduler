package com.github.ltsopensource.core.spi;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Robert HG (254963746@qq.com)on 12/23/15.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface SPI {

	/**
	 * config中的键值
	 */
	String key() default "";

	/**
	 * 默认扩展实现
	 */
	String dftValue() default "";

}
