package fof.daq.hub.common

import fof.daq.hub.model.Customer
import io.vertx.core.json.DecodeException
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import kotlin.reflect.KClass
import io.vertx.rxjava.core.eventbus.Message
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