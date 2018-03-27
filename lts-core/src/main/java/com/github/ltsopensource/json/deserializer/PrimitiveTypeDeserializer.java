package com.github.ltsopensource.json.deserializer;

import java.lang.reflect.Type;

import com.github.ltsopensource.core.commons.utils.PrimitiveTypeUtils;

/**
 * @author Robert HG (254963746@qq.com) on 12/30/15.
 */
public class PrimitiveTypeDeserializer implements Deserializer {

    public <T> T deserialize(Object object, Type type) {
        return PrimitiveTypeUtils.convert(object, type);
    }
}
