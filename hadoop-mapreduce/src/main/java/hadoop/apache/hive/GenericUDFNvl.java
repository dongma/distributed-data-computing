package hadoop.apache.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.log4j.Logger;

/**
 * 用GenericUDF类编写自定义函数nvl(),传入的值为null，那么就返回一个默认值
 *
 * @author Sam Ma
 * @date 2022/09/28
 */
@Description(name = "t_nvl", value = "_FUNC_(value, default_value) - Returns default value if value " +
        "is null else returns value",
        extended = "Example:\n " +
                " > SELECT _FUNC_(null, 'bla') FROM src limit 1;\n"
)
public class GenericUDFNvl extends GenericUDF {

//    hive (financials)>
//            > select t_nvl(1, 2) as col1,
//                 >   t_nvl(NULL, 5) as col2,
//                 >   t_nvl(null, "stuff") as col3 from littlebigdata limit 1;
//    OK
//    col1	col2	col3
//      1	 5	    stuff
    private static final Logger logger = Logger.getLogger(GenericUDFNvl.class);

    private transient GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    private transient ObjectInspector[] argumentOIs;

    public GenericUDFNvl() {
    }

    /**
     * 会被输入的每个参数调用，并最终传入到ObjectInspector中
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        argumentOIs = arguments;
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The operator 'T_NVL' accepts 2 arguments.");
        } else {
            returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
            if (returnOIResolver.update(arguments[0]) && returnOIResolver.update(arguments[1])) {
                return returnOIResolver.get();
            } else {
                throw new UDFArgumentTypeException(1, "The 1st and 2nd args of function T_NVL should have the same type, " +
                        "but they are different: \"" + arguments[0].getTypeName() + "\" and \""
                        + arguments[1].getTypeName() + "\"");
            }
        }
    }

    /**
     * [Need fix]: select nvl(null, 5) as col2 from littlebigdata limit 1;
     * FAILED: NullPointerException null
     *
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object retVal = null;
        try {
            retVal = returnOIResolver.convertIfNecessary(arguments[0].get(),
                    argumentOIs[0]);
        } catch (HiveException ex) {
            logger.error("convert arguments: " + ex.getMessage());
        }
        if (retVal == null) {
            retVal = returnOIResolver.convertIfNecessary(arguments[1].get(),
                    argumentOIs[1]);
        }
        return retVal;
    }

    /**
     * 在Hadoop task内部，在使用到这个函数时来展示调试信息
     *
     * @param children
     * @return
     */
    @Override
    public String getDisplayString(String[] children) {
        String buffer = "if " +
                children[0] +
                " is null " +
                "returns" +
                children[1];
        return buffer;
    }

}
