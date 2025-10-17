package com.skyflow.hive;

import com.skyflow.Skyflow;
import com.skyflow.errors.SkyflowException;
import com.skyflow.hive.utils.Helper;
import com.skyflow.vault.data.DetokenizeRequest;
import com.skyflow.vault.data.DetokenizeResponse;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

@Description(
        name = "detokenize",
        value = "_FUNC_(token) - Detokenizes a Skyflow token",
        extended = "Example:\n  > SELECT detokenize('asmgy88@lzugg.byn');"
)
public class DetokenizeUDF extends GenericUDF {
    private static Skyflow skyflowClient;
    private StringObjectInspector stringOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("detokenize() takes exactly one argument");
        }
        stringOI = (StringObjectInspector) arguments[0];
        skyflowClient = Helper.getConfigParametersAndInitialiseSDK();
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null) {
            return null;
        }
        String token = stringOI.getPrimitiveJavaObject(arguments[0].get());
        try {
            return new Text(callDetokenize(token));
        } catch (Exception e) {
            throw new HiveException("Error while detokenizing token: " + e.getMessage(), e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "detokenize(" + children[0] + ")";
    }

    private String callDetokenize(String token) throws Exception {
        try {
            ArrayList<String> tokens = new ArrayList<>();
            tokens.add(token);
            DetokenizeRequest request = DetokenizeRequest.builder().tokens(tokens).build();
            DetokenizeResponse response = skyflowClient.vault().bulkDetokenize(request);
            return response.getSuccess().get(0).getValue().toString();
        } catch (SkyflowException e) {
            throw new RuntimeException(e);
        }
    }
}
