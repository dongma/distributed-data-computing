package hadoop.apache.hive.format;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.ql.io.AbstractStorageFormatDescriptor;

import java.util.Set;

/**
 * 自定义storage类型为"geek"，并在inputFormat/outputFormat中指定class名称
 *
 * @author Sam Ma
 * @date 2022/09/29
 */
public class GeekStorageFormatDescriptor extends AbstractStorageFormatDescriptor {

    /*
     * 注册在org.apache.hadoop.hive.ql.io.StorageFormatDescriptor文件中，然后重
     *  新打hive jar包
     */
    @Override
    public Set<String> getNames() {
        return ImmutableSet.of("geek");
    }

    @Override
    public String getInputFormat() {
        return GeekTextInputFormat.class.getName();
    }

    @Override
    public String getOutputFormat() {
        return GeekTextOutputFormat.class.getName();
    }

}
