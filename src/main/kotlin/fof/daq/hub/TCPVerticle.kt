package fof.daq.hub

import fof.daq.hub.common.logger
import fof.daq.hub.common.value
import fof.daq.hub.tcp.TcpBridgeHandler
import io.vertx.core.json.JsonObject
import io.vertx.ext.bridge.BridgeOptions
import io.vertx.ext.bridge.PermittedOptions
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.ext.eventbus.bridge.tcp.TcpEventBusBridge
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component


/**
 * Web Sockjs 接口服務
 * */
@Component
class TCPVerticle : AbstractVerticle() {

    private val log = logger(this::class)

    @Autowired
    @Qualifier("config")
    private lateinit var config: JsonObject

    @Autowired
    private lateinit var tcpBridgeHandler: TcpBridgeHandler

    @Throws(Exception::class)
    override fun start() {
        val opt = BridgeOptions()
                .addInboundPermitted(PermittedOptions().setAddress(Address.WEB.PROXY).setAddressRegex(Address.CW.PATH  + "*"))
                .addOutboundPermitted(PermittedOptions().setAddressRegex(Address.CW.PATH + "*"))

        val bridge = TcpEventBusBridge.create(vertx, opt, null, tcpBridgeHandler)
        val port = config.value("TCP.PORT", 8080)
        bridge.listen(port){ ar ->
            if (ar.succeeded()) {
                log.info("Success start TCP:$port")
            } else {
                log.error(ar.cause())
            }
        }
    }
}