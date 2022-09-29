package hadoop.apache.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMkCollectionEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用collect_set()对元素进行收集，类似与mysql中的group_concat()函数
 *
 * @author Sam Ma
 * @date 2022/09/29
 */
@Description(name = "collect", value = "_FUNC_(x) - Returns a list of objects. " +
    "CAUTION will easily OOM on large data sets")
public class GenericUDAFCollect extends AbstractGenericUDAFResolver {

    static final Logger LOGGER = LoggerFactory.getLogger(GenericUDAFCollect.class);

    /*
     * 2022-0929 [need-to-fix]:
     * Job running in-process (local Hadoop)
     * 2022-09-29 10:33:41,549 Stage-1 map = 0%,  reduce = 0%
     * Ended Job = job_local745077883_0002 with errors
     * Error during job, obtaining debugging information...
     * FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask
     * MapReduce Jobs Launched:
     */
    public GenericUDAFCollect() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
                    + parameters[0].getTypeName() + " was passed as parameter 1.");
        }
        return new GenericUDAFMkCollectionEvaluator();
    }

}
