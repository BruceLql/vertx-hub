package fof.daq.hub.tcp

import fof.daq.hub.common.logger
import io.vertx.core.Handler
import io.vertx.rxjava.ext.eventbus.bridge.tcp.BridgeEvent
import org.springframework.stereotype.Controller

/**
 * tcp 桥接访问处理控制器
 * */
@Controller
class TcpBridgeHandler : Handler<BridgeEvent> {
    private val log = logger(this::class)
    override fun handle(event: BridgeEvent) {
        event.complete(true)
    }
}