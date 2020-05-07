package fof.daq.hub.common.utils

import io.vertx.core.logging.LoggerFactory

class LogUtils(clazz: Class<*>){
    private val logger = LoggerFactory.getLogger(clazz)
    fun info(mobile: String, message: String, params: Any? = null) {
        this.record("info", mobile, message, params)
    }

    fun error(mobile: String, message: String, error: Throwable, params: Any? = null) {
        this.record("error", mobile, message, params, error)
    }

    /**
     * 追踪一般用于响应事件执行前,（用户端不显示）
     * */
    fun trace(mobile: String, message: String, params: Any? = null) {
        this.record("trace", mobile, message, params)
    }
    /**
     * 追踪返回的错误结果（用户端不显示）
     * */
    fun failed(mobile: String, message: String, error: Throwable, params: Any? = null) {
        this.record("trace", mobile, message, params, error)
    }

    /**
     * 数据整理
     * 类型： info / error / debug / warn / trace
     * */
    private fun record(type:String, mobile: String, message: String, params: Any? = null, error: Throwable? = null) {
        var content =  "\"type\":\"$type\",\"mobile\":\"$mobile\",\"time\":\"${System.currentTimeMillis()}\",\"message\":\"$message\""
            error?.also { content += ",\"error\":\"[${it.javaClass.simpleName}] ${it.message}\"" }
            params?.also { content += ",\"params\":\"$it\"" }
        logger.info("{$content}")
    }

}