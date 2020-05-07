package fof.daq.hub.tcp

import fof.daq.hub.Address
import fof.daq.hub.common.customer
import fof.daq.hub.common.logger
import fof.daq.hub.common.utils.LogUtils
import fof.daq.hub.common.value
import fof.daq.hub.web.handler.HubProxyHandler
import io.vertx.core.Handler
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.rxjava.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.bridge.BridgeEventType
import io.vertx.rxjava.core.buffer.Buffer.buffer
import io.vertx.rxjava.core.eventbus.EventBus
import io.vertx.rxjava.core.net.NetSocket
import io.vertx.rxjava.ext.eventbus.bridge.tcp.BridgeEvent
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import rx.Observable

/**
 * tcp 桥接访问处理控制器
 * */
@Controller
class TcpBridgeHandler @Autowired constructor(val vertx: Vertx, val eb: EventBus) : Handler<BridgeEvent> {
    private val log = logger(this::class)
    private val logUtils = LogUtils(this::class.java)
    private val listHandlerID = mutableMapOf<String, JsonObject>()
    private val listUUID = mutableMapOf<String, Long>()
    override fun handle(event: BridgeEvent) {
        event.socket().closeHandler {
            event.socket()?.writeHandlerID()?.also { id ->
                listHandlerID[id]?.also { headers ->
                   this.closeHandler(id, headers)
                }
            }
        }
        Observable.just(event)
                .flatMap(this::registerEvent)
                .subscribe({
                    it.complete(true)
                }, { err ->
                    replyError(event.socket(), err)
                    event.fail(err)
                    log.error(err)
                })
    }

    /**
     * 服务注册绑定
     * */
    private fun registerEvent(event: BridgeEvent): Observable<BridgeEvent> {
        if (event.type() != BridgeEventType.REGISTER) return Observable.just(event)
        return try {
            val headers = event.rawMessage.value<JsonObject>("headers") ?: throw NullPointerException("Headers is null")
            val uuid = headers.value<String>("uuid") ?: throw NullPointerException("UUID is null")
            val mobile = headers.value<String>("mobile") ?: throw NullPointerException("Mobile is null")
            val address = event.rawMessage.value<String>("address")
            val host = event.socket()?.remoteAddress()?.host()
            val port = event.socket()?.remoteAddress()?.port()
            logUtils.info(mobile, "[服务注册] 注册地址:$address / IP[$host:$port]", headers)
            event.socket()?.writeHandlerID()?.also { id ->
                listUUID[uuid]?.let(vertx::cancelTimer).also {
                    logUtils.trace(mobile, "[服务注册] 取消关闭已有定时器UUID[$uuid]", headers)
                }
                listUUID.remove(uuid)
                listHandlerID[id] = headers
                logUtils.trace(mobile, "[服务注册] 绑定ID[$id]", headers)
            }
            Observable.just(event)
        } catch (e: Exception) {
            Observable.error(e)
        }
    }

    /**
     * 关闭事件通知
     * */
    private fun closeHandler(id:String, headers: JsonObject) {
        val uuid = headers.value<String>("uuid")
        val mobile = headers.value<String>("mobile")
        if (uuid != null && mobile != null) {
            val delay: Long = 1000 * 6 // 默认一分钟
            logUtils.trace(mobile, "[服务关闭] 设定${delay}ms触发服务关闭通知", headers)
            listUUID[uuid] = vertx.setTimer(delay){
                listHandlerID.remove(id)
                listUUID.remove(uuid)
                logUtils.info(mobile, "[服务关闭] 触发关闭通知地址:${Address.WEB.PROXY}", headers)
                val options = DeliveryOptions().customer(headers, 5)
                eb.send<JsonObject>(Address.WEB.PROXY, Address.WEB.event(Address.Event.CLOSE), options){
                    if (it.succeeded()) {
                        log.info("[Crawler close] result:${it.result()}")
                    } else {
                        log.error(it.cause())
                    }
                }
            }
        }
    }


    companion object {
        /**
         * 返回错误信息
         * */
        fun replyError(socket: NetSocket, err: Throwable) {
            val envelope = JsonObject().put("type", "err").put("body", err.message)
            socket.write(buffer(envelope.encode()))
        }
    }
}