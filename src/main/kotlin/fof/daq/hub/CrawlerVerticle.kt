package fof.daq.hub

import fof.daq.hub.common.logger
import io.vertx.core.json.JsonObject
import io.vertx.ext.bridge.BridgeEventType
import io.vertx.ext.bridge.PermittedOptions
import io.vertx.ext.web.handler.sockjs.SockJSHandler
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.eventbus.EventBus
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.handler.StaticHandler
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

/**
 * 采集服务
 * */
@Component
class CrawlerVerticle : AbstractVerticle() {

    private val log = logger(this::class)

    @Autowired
    @Qualifier("config")
    private lateinit var config: JsonObject

    @Autowired
    private lateinit var eb: EventBus

    /**
     * 注入全局路由
     * */
    @Autowired
    private lateinit var router: Router


    @Throws(Exception::class)
    override fun start() {
        router.route().handler(StaticHandler.create())
    }
}