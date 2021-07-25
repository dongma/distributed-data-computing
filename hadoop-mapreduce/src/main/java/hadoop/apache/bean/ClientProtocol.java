package hadoop.apache.bean;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @author Sam Ma
 * @date 2021/07/25
 * 定义ClientProtocol协议版本、远程调用的方法
 */
public interface ClientProtocol extends VersionedProtocol {

    /** 定义protocol版本号，不通版本的RPC client与Server间不能通信 */
    public static final long versionID = 1L;

    /** 根据学生学号studentId获取其姓名name */
    String getActualName(String studentId);
}
