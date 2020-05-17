package fof.daq.hub.model

import com.fasterxml.jackson.annotation.JsonInclude
import io.vertx.core.json.JsonArray
import rx.Observable

/**
 * 服务封装类
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class Server(
        var host: String,                                //IP
        var port: Int,                                   //port
        var server_name: String,                         //服务名
        var timestamp: Long,                             //最近一次时间戳
        var tags: Int,                                   //存放ISP运营商类型 （计算tags值）
        var version: String,                             //版本
        var status: Int,                                 //服务状态
        var switch: Int,                                 //开通状态 0关，1开 （注册初始化为开通状态）
        var created_at: Long,                            //创建时间（第一次创建时间）
        var url:String                                   //请求地址
) : AbstractModel() {


    companion object {
        //py 机器列表
        const val tableName = "py_server"

        /**
         * 存储py server
         */
        const val PY_SERVER_KEY = "_PY_SERVER_LIST_"


        /**
         * 运营商类型
         */
        enum class ISP(var code: Int, var field: String, var message: String) {
            CUCC(1, "CUCC", "移动"),
            CTCC(2, "CTCC", "电信"),
            CMCC(4, "CMCC", "联通")
        }

        /**
         *  计算
         */
        fun calcTags(listIsp: List<String>): Int {
           return listIsp.sumBy {
                when (it) {
                    ISP.CMCC.field -> ISP.CMCC.code
                    ISP.CTCC.field -> ISP.CTCC.code
                    ISP.CUCC.field -> ISP.CUCC.code
                    else -> 0
                }
            }
        }

        /**
         * 转换
         */
        fun calcSQL(num :Int): String {
            return when(num){
                ISP.CMCC.code -> "4,5,6,7"
                ISP.CTCC.code -> "2,3,6,7"
                ISP.CUCC.code -> "1,3,5,7"
                else -> ""
            }
        }


        /**
         * 获取计算结果
         */
        fun getTags(num: Int): List<String> {
            return when (num) {
                ISP.CMCC.code -> listOf(ISP.CMCC.field)
                ISP.CTCC.code -> listOf(ISP.CTCC.field)
                ISP.CUCC.code -> listOf(ISP.CUCC.field)
                in 0..(ISP.CMCC.code + ISP.CTCC.code) -> {
                    listOf(ISP.CMCC.field, ISP.CTCC.field)
                }
                in 0..(ISP.CMCC.code + ISP.CUCC.code) -> {
                    listOf(ISP.CMCC.field, ISP.CUCC.field)
                }
                in 0..(ISP.CTCC.code + ISP.CUCC.code) -> {
                    listOf(ISP.CTCC.field, ISP.CUCC.field)
                }
                (ISP.CMCC.code + ISP.CTCC.code + ISP.CUCC.code) -> {
                    listOf(ISP.CMCC.field, ISP.CTCC.field, ISP.CUCC.field)
                }
                else -> throw NullPointerException("isp not matching condition")
            }
        }


        @JvmStatic
        fun main(args: Array<String>) {
            val array = JsonArray().add("CUCC").add("CTCC").add("CMCC")
            //计算tags 值
            val calcTags = this.calcTags(listOf("CMCC","CTCC"))
            println("计算tags列表的值       : $calcTags")
            //获取tags 列表
            val tags = this.getTags(calcTags)
            println("通过calcTags值获取列表 : $tags")

        }
    }
}
