package fof.daq.hub

import fof.daq.hub.common.logger
import fof.daq.hub.common.value
import fof.daq.hub.web.handler.InitCrawlerHandler
import fof.daq.hub.web.WebSocketAuthHandler
import fof.daq.hub.web.WebSocketBridgeHandler
import fof.daq.hub.web.handler.HubProxyHandler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.bridge.PermittedOptions
import io.vertx.ext.web.handler.sockjs.BridgeOptions
import io.vertx.ext.web.handler.sockjs.SockJSHandler
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.eventbus.EventBus
import io.vertx.rxjava.ext.auth.jwt.JWTAuth
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.handler.CorsHandler
import io.vertx.rxjava.ext.web.handler.JWTAuthHandler
import io.vertx.rxjava.ext.web.handler.SessionHandler
import io.vertx.rxjava.ext.web.handler.StaticHandler
import io.vertx.rxjava.ext.web.sstore.LocalSessionStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

/**
 * Web Sockjs 接口服務
 * */
@Component
class WebVerticle : AbstractVerticle() {

    private val log = logger(this::class)

    @Autowired
    @Qualifier("config")
    private lateinit var config: JsonObject

    /** 通信总线 **/
    @Autowired
    private lateinit var eb: EventBus

    /** 授权签名证书 **/
    @Autowired
    private lateinit var jwt: JWTAuth

    /** 注入全局路由 **/
    @Autowired
    private lateinit var router: Router

    /** 权限路由转换 **/
    @Autowired
    private lateinit var webSocketAuthHandler: WebSocketAuthHandler

    /** SockJs 访问处理器 **/
    @Autowired
    private lateinit var webSocketBridgeHandler: WebSocketBridgeHandler

    /** 初始化选择采集服务 **/
    @Autowired
    private lateinit var initCrawlerHandler: InitCrawlerHandler

    /**代理中心*/
    @Autowired
    private lateinit var hubProxyHandler: HubProxyHandler

    @Throws(Exception::class)
    override fun start() {
        router.route().handler(StaticHandler.create())
        // 开始session
        router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)))
        /**
         * 许可跨域授权 (注：直接设置全局*是无效的，必须试用绝对路径地址)
         * */
        val allowedHeaders =  mutableSetOf<String>()
            allowedHeaders.add("x-requested-with")
            allowedHeaders.add("Access-Control-Allow-Origin")
            allowedHeaders.add("origin")
            allowedHeaders.add("Content-Type")
            allowedHeaders.add("accept")
            allowedHeaders.add("X-PINGARUNER")

        config.value<JsonArray>("CORS")?.also {
            list ->
            list.forEach {
                router.route("/socket/*").handler(CorsHandler.create(it.toString()).allowedHeaders(allowedHeaders).allowCredentials(true))
                log.info("Add Cors address: $it")
            }
        }

        // 设置通信地址权限
        val opts = BridgeOptions()
                // 许可访问权限地址
                .addInboundPermitted(PermittedOptions().setAddressRegex(Address.WEB.PATH + "*"))
                // 许可输出权限地址
                .addOutboundPermitted(PermittedOptions().setAddressRegex("FOF.*"))
        log.info("[INBOUND]: ${opts.inboundPermitteds.map { "${it.addressRegex}, ${it.address}" }}")
        log.info("[OUTBOUND]: ${opts.outboundPermitteds.map { "${it.addressRegex}, ${it.address}" }}")

        // 注册并监听sock通信
        val sockHandler = SockJSHandler.create(vertx.delegate)
            sockHandler.bridge(opts, webSocketBridgeHandler) // 版本兼容问题导致集群模式rx桥接错误

        // 提取url授权token并限制访问权限
        router.route("/socket/*").handler(webSocketAuthHandler).handler(JWTAuthHandler.create(jwt))
        router.route("/socket/*").delegate.handler(sockHandler)

        // 注册本地初始化服务
        log.info("注册本地初始化服务: ${Address.WEB.INIT}")
        this.eb.localConsumer<JsonObject>(Address.WEB.INIT).handler(initCrawlerHandler)
        log.info("注册全局代理中心: ${Address.WEB.PROXY}")
        this.eb.consumer<JsonObject>(Address.WEB.PROXY).handler(hubProxyHandler)

        // 启动HTTP服务
        val port = config.value("WEB.PORT", 80)
        vertx.createHttpServer().requestHandler(router).listen(port){
            if (it.succeeded()) {
                log.info("Success start WEB port:${it.result().actualPort()}")
            } else {
                log.error(it.cause())
            }
        }
    }
}
