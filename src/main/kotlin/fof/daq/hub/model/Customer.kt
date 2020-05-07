package fof.daq.hub.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import fof.daq.hub.Address
import fof.daq.hub.common.value
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.MultiMap

/**
 * 客户基础模型结构
 * */
@JsonInclude(JsonInclude.Include.NON_NULL) // 忽略NULL数据否则 headers 无法转换
data class Customer (
        var uuid: String,            // 唯一标识(由token生成的MD5值)
        var mobile: String,          // 手机号码
        var isp: String,             // 服务运营商
        var sessionId: String? = null, // 会话ID(对应session oldId)
        var oid: String? = null,     // 订单ID(order)预留
        var sid: String? = null,     // 商户ID(store)预留
        var pid: String? = null,     // 平台ID(platform)预留
        var mid: String? = null,     // 采集服务返回的(MongodbID)预留
        var headers: String? = null,    // 临时存放header信息
        var createdAt: Long? = null  // 建立时间
): AbstractModel() {
    @JsonCreator
    constructor(
            @JsonProperty("uuid") uuid:String,
            @JsonProperty("mobile") mobile:String,
            @JsonProperty("isp") isp:String,
            @JsonProperty("sessionId") sessionId:String?
    ): this(uuid, mobile, isp, sessionId, null)

    fun reply(): JsonObject{
        return this.toJson().apply { this.remove("headers") }
    }

    /**
     * 过滤传递至采集服务的参数
     * */
    fun toCrawler(): JsonObject{
        return this.toJson().apply { this.remove("headers"); this.remove("createdAt") }
    }

    companion object {
        /**
         * 根据header的转实体
         * */
        fun create(headers: MultiMap): Customer{
            return JsonObject(headers.associate { Pair(it.key, it.value) }.toMap()).mapTo(Customer::class.java)
        }

    }

    /**
     * 运营商分类
     * */
    enum class ISP{
        CMCC,
        CUCC,
        CTCC
    }
}