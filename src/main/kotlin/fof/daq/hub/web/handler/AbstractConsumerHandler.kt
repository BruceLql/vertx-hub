package fof.daq.hub.web.handler

import fof.daq.hub.common.logger
import fof.daq.hub.model.Customer
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.eventbus.Message


abstract class AbstractConsumerHandler : Handler<Message<JsonObject>> {
    val log = logger(this::class)
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
                JsonObject(headers.toString()).forEach { item ->
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
