package fof.daq.hub

import fof.daq.hub.common.enums.ServerState
import fof.daq.hub.common.logger
import fof.daq.hub.common.value
import fof.daq.hub.service.HeartBeatService
import fof.daq.hub.web.handler.InitCrawlerHandler
import fof.daq.hub.web.WebSocketAuthHandler
import fof.daq.hub.web.WebSocketBridgeHandler
import fof.daq.hub.web.handler.HubProxyHandler
import fof.daq.hub.web.handler.SocketProxyHandler
import fof.daq.hub.web.handler.heartbeat.ValidateRequestHandler
import io.vertx.core.json.JsonObject
import io.vertx.ext.bridge.PermittedOptions
import io.vertx.ext.web.handler.sockjs.BridgeOptions
import io.vertx.ext.web.handler.sockjs.SockJSHandler
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.eventbus.EventBus
import io.vertx.rxjava.ext.auth.jwt.JWTAuth
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.handler.*
import io.vertx.rxjava.ext.web.sstore.LocalSessionStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import rx.Observable
import java.util.concurrent.TimeUnit

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

    /**请求校验*/
    @Autowired
    private lateinit var validateRequestHandler: ValidateRequestHandler

    @Autowired
    private lateinit var heartBeatService: HeartBeatService

    /**代理中心*/
    @Autowired
    private lateinit var socketProxyHandler: SocketProxyHandler

    /**
     * 设置超时时间 (单位毫秒)
     */
    private val TIME_DIFF = 1000 * 60 * 3

    /**
     * 定时器间隔 （单位秒）
     */
    private val TIME_INTERVAL: Long = 10

    @Throws(Exception::class)
    override fun start() {
        router.route().handler(StaticHandler.create())
        router.route("/heartbeat/*").handler(BodyHandler.create())
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

        config.value<String>("CORS")?.also {
            router.route("/socket/*").handler(CorsHandler.create(".*").allowedHeaders(allowedHeaders).allowCredentials(true))
            log.info("Add Cors address: $it")
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

        //心跳注册，请求校验
        router.post("/heartbeat/register").handler(validateRequestHandler)
        router.post("/heartbeat/register").handler(validateRequestHandler)
        //定时执行心跳检测
//        scheduleReFlushServerState()

        // 注册本地初始化服务
        log.info("注册本地初始化服务: ${Address.WEB.INIT}")
        this.eb.localConsumer<JsonObject>(Address.WEB.INIT).handler(initCrawlerHandler)
        log.info("注册py代理中心: ${Address.WEB.PROXY}")
        this.eb.consumer<JsonObject>(Address.WEB.PROXY).handler(hubProxyHandler)

        log.info("注册前端代理中心: ${Address.WEB.SOCKET_PROXY}")
        this.eb.consumer<JsonObject>(Address.WEB.SOCKET_PROXY).handler(socketProxyHandler)

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

    /**
     * 定时更新服务状态
     */
    private fun scheduleReFlushServerState() {
        Observable.defer {
            Observable.timer(TIME_INTERVAL, TimeUnit.SECONDS)
                    .doOnError { it.printStackTrace() }
                    .flatMap {
                log.info("[定时更新服务器状态执行] >>>>>>>>>")
                //查询出正常状态的服务器
                heartBeatService.listHeartBeatByStatus().map { serverList ->
                    if (!serverList.isNullOrEmpty()) {
                        val currentTime = System.currentTimeMillis()
                        //获取超时的服务器
                        var timeOutList = serverList.filter { (currentTime - it.value<Long>("timestamp",0)) > TIME_DIFF }.toList()
                        if (timeOutList.isNotEmpty()) {
                            var hosts = timeOutList.map {
                                it.value<String>("host") + ":" + it.value<Number>("port")
                            }.toList()
                            log.info("[查询到mongodb超过3分钟未更新的服务] list: $hosts")
                            val list =
                                    (timeOutList.indices).map{ index -> heartBeatService.updateByStatus(timeOutList[index].value("host",""), timeOutList[index].value<Number>("port",0), ServerState.DOWN) }
                            Observable.concat(list).subscribe({},{ it.printStackTrace() })
                        }
                    }
                }
            }
        }.repeat().subscribe()
    }
}
