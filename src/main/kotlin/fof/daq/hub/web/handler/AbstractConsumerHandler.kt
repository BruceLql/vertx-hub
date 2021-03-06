package fof.daq.hub.web.handler

import fof.daq.hub.common.logger
import fof.daq.hub.model.Customer
import io.vertx.core.Handler
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.eventbus.Message
import rx.Subscription


abstract class AbstractConsumerHandler : Handler<Message<JsonObject>> {
    val log = logger(this::class)
    /**
     * 初始化加载列表
     * */
    val listInitObservable = mutableMapOf<String, Subscription>()
    abstract fun consumer(customer: Customer?, message: Message<JsonObject>)

    override fun handle(message: Message<JsonObject>) {
        val customer = try {
            Customer.create(message.headers())
        } catch (e: Exception) {
            log.error(e)
            null
        }
        /**
         * 复原 headers 信息
         * */
        customer?.headers?.also { headers ->
            try {
                message.headers().clear()
                JsonObject(headers).forEach { item ->
                    item.value?.toString()?.also {
                        message.headers().add(item.key, it)
                    }
                }
            } catch (e: Exception){
                log.error(e)
            }
        }
        this.consumer(customer, message)
    }

}
