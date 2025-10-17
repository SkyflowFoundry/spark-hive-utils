package com.skyflow.hive;

import com.skyflow.hive.utils.BaseSkyflowTest;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DetokenizeUDFTest extends BaseSkyflowTest {
    
    private DetokenizeUDF udf;
    private ObjectInspector[] inspectors;

    @BeforeEach
    public void setup() {
        udf = new DetokenizeUDF();
        inspectors = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };
    }

    @Test
    void testInitializeWithNoArguments() {
        assertThrows(UDFArgumentException.class, () -> udf.initialize(new ObjectInspector[]{}));
    }

    @Test
    void testInitializeWithTooManyArguments() {
        assertThrows(UDFArgumentException.class, () -> udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        }));
    }

    @Test
    void testInitializeWithValidArgument() throws UDFArgumentException {
        String testConfig = "{\"vaultId\":\"test-vault\",\"clusterId\":\"test-cluster\",\"env\":\"PROD\",\"credentials\":\"{\\\"accessToken\\\":\\\"test-token\\\"}\"}";
        when(mockConf.get("skyflow.config")).thenReturn(testConfig);
        when(mockConf.get("skyflow.config.file")).thenReturn(null);
        
        ObjectInspector result = udf.initialize(inspectors);
        assertEquals(PrimitiveObjectInspectorFactory.writableStringObjectInspector, result);
    }

    @Test
    void testEvaluateWithNullInput() throws HiveException {
        String testConfig = "{\"vaultId\":\"test-vault\",\"clusterId\":\"test-cluster\",\"env\":\"PROD\",\"credentials\":\"{\\\"accessToken\\\":\\\"test-token\\\"}\"}";
        when(mockConf.get("skyflow.config")).thenReturn(testConfig);
        when(mockConf.get("skyflow.config.file")).thenReturn(null);
        
        udf.initialize(inspectors);
        DeferredObject nullObject = new DeferredObject() {
            @Override
            public void prepare(int version) throws HiveException {}

            @Override
            public Object get() throws HiveException {
                return null;
            }
        };

        Object result = udf.evaluate(new DeferredObject[]{nullObject});
        assertNull(result);
    }

    @Test
    void testGetDisplayString() {
        String[] children = new String[]{"token123"};
        assertEquals("detokenize(token123)", udf.getDisplayString(children));
    }
}