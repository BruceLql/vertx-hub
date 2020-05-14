package fof.daq.hub.common

import fof.daq.hub.common.enums.HttpStatus
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.DecodeException
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.rxjava.core.http.HttpServerResponse
import kotlin.reflect.KClass

/**
 * 附加工具
 * */

/**
 * 日志接口方法
 */
fun logger(clz: KClass<*>): Logger {
    return LoggerFactory.getLogger(clz.qualifiedName)
}

/**
 * JSON数据格式转换扩展
 * */
inline fun <reified T> JsonObject?.value(key: String): T? {
    if (this == null) return null
    if (!this.containsKey(key)) return null
    val value = this.getValue(key)
    return when(value){
        is T -> value
        else -> null
    }
}

inline fun <reified T> JsonObject?.value(key: String, default: T): T {
    return this.value<T>(key) ?: default
}

@Throws(DecodeException::class)
inline fun <reified T> JsonObject.toEntity(): T {
    return Json.prettyMapper.convertValue(this, T::class.java)
}

/**
 * 封装客户信息转headers
 * */
fun DeliveryOptions.customer(customerJson: JsonObject, timeOut: Long = 20): DeliveryOptions {
    this.sendTimeout = timeOut * 1000 // 限定发送超时时间默认20秒
    customerJson.forEach { (key, value) ->
        if (key != "headers") {
            value?.also { this.addHeader(key, it.toString()) }
        }
    }
    return this
}


/**
 * 成功，自定义message 且无返回内容
 */
fun HttpServerResponse.success(message: String){
    commonJson(HttpStatus.OK,message,null)
}

/**
 * 成功有返回内容
 */
fun HttpServerResponse.success(data:Any){
    commonJson(HttpStatus.OK,null,data)
}

/**
 * 成功无返回内容
 */
fun HttpServerResponse.success(){
    commonJson(HttpStatus.OK,null,null)
}
/**
 * 错误返回
 */
fun HttpServerResponse.error(httpStatus: HttpStatus){
    commonJson(httpStatus,null,null)
}

/**
 * 错误返回，带信息
 */
fun HttpServerResponse.error(httpStatus: HttpStatus,message: String){
    commonJson(httpStatus,message,null)
}

private fun HttpServerResponse.commonJson(httpStatus: HttpStatus,message: String?,data:Any?){
    this.putHeader("Content-Type", "application/json; charset=utf-8")
    this.statusCode = httpStatus.code
    this.statusMessage = httpStatus.message
    var result = JsonObject()
            .put("code",httpStatus.code)
            .put("msg",message?:httpStatus.message)
    if(data != null){
        result.put("data",data)
    }
    this.end(result.encode())
}
