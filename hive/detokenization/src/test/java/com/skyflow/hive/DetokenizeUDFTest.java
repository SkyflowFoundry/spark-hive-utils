package com.skyflow.hive;

import com.skyflow.Skyflow;
import com.skyflow.errors.SkyflowException;
import com.skyflow.hive.utils.BaseSkyflowTest;
import com.skyflow.hive.utils.Helper;
import com.skyflow.vault.data.DetokenizeRequest;
import com.skyflow.vault.data.DetokenizeResponse;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DetokenizeUDFTest extends BaseSkyflowTest {

    private DetokenizeUDF udf;
    private ObjectInspector[] inspectors;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Skyflow mockSkyflowClient;

    @Mock
    private DetokenizeResponse mockDetokenizeResponse;

    private MockedStatic<Helper> mockedHelper;

    @BeforeEach
    public void setup() {
        udf = new DetokenizeUDF();
        inspectors = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };
        mockedHelper = mockStatic(Helper.class);
    }

    @AfterEach
    public void tearDown() {
        if (mockedHelper != null) {
            mockedHelper.close();
        }
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
    void testGetDisplayString() {
        String[] children = new String[]{"token123"};
        assertEquals("detokenize(token123)", udf.getDisplayString(children));
    }


    @Test
    void testInitializeWithValidArgument() throws UDFArgumentException {
        mockedHelper.when(Helper::getConfigParametersAndInitialiseSDK).thenReturn(mockSkyflowClient);

        ObjectInspector result = udf.initialize(inspectors);
        assertEquals(PrimitiveObjectInspectorFactory.writableStringObjectInspector, result);

        mockedHelper.verify(Helper::getConfigParametersAndInitialiseSDK, times(1));
    }

    @Test
    void testEvaluateWithNullInput() throws HiveException {
        mockedHelper.when(Helper::getConfigParametersAndInitialiseSDK).thenReturn(mockSkyflowClient);
        udf.initialize(inspectors);

        DeferredObject nullObject = new DeferredObject() {
            @Override
            public void prepare(int version) {}
            @Override
            public Object get() { return null; }
        };

        Object result = udf.evaluate(new DeferredObject[]{nullObject});
        assertNull(result);
    }

    @Test
    void testEvaluateSuccess() throws HiveException, SkyflowException {
        mockedHelper.when(Helper::getConfigParametersAndInitialiseSDK).thenReturn(mockSkyflowClient);
        udf.initialize(inspectors);

        String inputToken = "token123";
        String expectedValue = "detokenized_value";

        DeferredObject inputObject = new DeferredObject() {
            @Override
            public void prepare(int version) {}
            @Override
            public Object get() { return inputToken; }
        };

        when(mockSkyflowClient.vault().bulkDetokenize(any(DetokenizeRequest.class)))
                .thenReturn(mockDetokenizeResponse);

        try {
            java.lang.reflect.Method getSuccessMethod = DetokenizeResponse.class.getMethod("getSuccess");
            java.lang.reflect.ParameterizedType listType = (java.lang.reflect.ParameterizedType) getSuccessMethod.getGenericReturnType();
            Class<?> itemClass = (Class<?>) listType.getActualTypeArguments()[0];

            Object mockItem = mock(itemClass);

            when(itemClass.getMethod("getValue").invoke(mockItem)).thenReturn(expectedValue);

            when(mockDetokenizeResponse.getSuccess()).thenReturn((List) Collections.singletonList(mockItem));

        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to setup dynamic mocks: " + e.getMessage());
        }

        Object result = udf.evaluate(new DeferredObject[]{inputObject});

        assertNotNull(result);
        assertEquals(expectedValue, result.toString());
    }
    @Test
    void testEvaluateFailureFromSkyflow() throws HiveException, SkyflowException {
        mockedHelper.when(Helper::getConfigParametersAndInitialiseSDK).thenReturn(mockSkyflowClient);
        udf.initialize(inspectors);

        DeferredObject inputObject = new DeferredObject() {
            @Override
            public void prepare(int version) {}
            @Override
            public Object get() { return "bad_token"; }
        };

        when(mockSkyflowClient.vault().bulkDetokenize(any(DetokenizeRequest.class)))
                .thenThrow(new SkyflowException(500, "API Failure"));

        HiveException exception = assertThrows(HiveException.class, () -> {
            udf.evaluate(new DeferredObject[]{inputObject});
        });

        assertTrue(exception.getMessage().contains("API Failure"));
    }
}