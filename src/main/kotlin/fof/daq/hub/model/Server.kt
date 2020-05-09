package fof.daq.hub.model

import com.fasterxml.jackson.annotation.JsonInclude
import io.vertx.core.json.JsonArray

/**
 * 服务封装类
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
open class Server(
        var host: String,                                //IP
        var port: Int,                                   //port
        var server_name: String,                         //服务名
        var timestamp: Long,                             //最近一次时间戳
        var tags: JsonArray? = null,                     //标签
        var version: String,                             //版本
        var status: Int,                                 //服务状态
        var switch: Int,                                 //开通状态 0关，1开 （注册初始化为开通状态）
        var created_at: Long                             //创建时间（第一次创建时间）
) : AbstractModel() {

    companion object {
        //py 机器列表
        var tableName = "py_server"
    }
}

