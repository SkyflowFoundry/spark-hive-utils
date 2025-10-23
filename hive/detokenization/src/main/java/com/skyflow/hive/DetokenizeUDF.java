package com.skyflow.hive;

import com.skyflow.Skyflow;
import com.skyflow.errors.SkyflowException;
import com.skyflow.hive.errors.ErrorMessage;
import com.skyflow.hive.logs.ErrorLogs;
import com.skyflow.hive.utils.Helper;
import com.skyflow.utils.Utils;
import com.skyflow.utils.logger.LogUtil;
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

/**
 * A Hive User-Defined Function (UDF) that detokenizes Skyflow tokens to retrieve their original values.
 * This UDF takes a single token as input and returns its detokenized value using the Skyflow SDK.
 * It requires proper configuration through Hive session parameters for Skyflow credentials and vault settings.
 */

@Description(
        name = "detokenize",
        value = "_FUNC_(token) - Detokenizes a Skyflow token",
        extended = "Example:\n  > SELECT detokenize('asmgy88@lzugg.byn');"
)
public class DetokenizeUDF extends GenericUDF {
    private static Skyflow skyflowClient;
    private StringObjectInspector stringOI;

    /**
     * Initializes the UDF by validating arguments and setting up the Skyflow client.
     *
     * @param arguments Array of ObjectInspectors for the UDF arguments
     * @return ObjectInspector for the return type
     * @throws UDFArgumentException if the number of arguments is invalid
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            LogUtil.printErrorLog(ErrorLogs.INVALID_ARGUMENT_COUNT.getLog());
            throw new UDFArgumentException(ErrorMessage.InvalidArgumentCount.getMessage());
        }
        stringOI = (StringObjectInspector) arguments[0];
        skyflowClient = Helper.getConfigParametersAndInitialiseSDK();
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    /**
     * Evaluates the UDF for a given set of arguments.
     * Takes a token string and returns its detokenized value.
     *
     * @param arguments Array of DeferredObject containing the input arguments
     * @return Object The detokenized value as Text, or null if input is null
     * @throws HiveException If detokenization fails
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null) {
            return null;
        }
        String token = stringOI.getPrimitiveJavaObject(arguments[0].get());
        try {
            return new Text(callDetokenize(token));
        } catch (Exception e) {
            LogUtil.printErrorLog(ErrorLogs.DETOKENIZE_FAILED.getLog());
            throw new HiveException(Utils.parameterizedString(ErrorMessage.DetokenizationFailed.getMessage(), e.getMessage()), e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "detokenize(" + children[0] + ")";
    }

    /**
     * Helper method that performs the actual detokenization using Skyflow SDK.
     *
     * @param token The token to be detokenized
     * @return String The detokenized value
     * @throws Exception If detokenization fails
     */
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
