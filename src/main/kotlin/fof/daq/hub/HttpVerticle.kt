package fof.daq.hub

import fof.daq.hub.common.enums.ServerState
import fof.daq.hub.common.logger
import fof.daq.hub.common.value
import fof.daq.hub.model.Server
import fof.daq.hub.web.handler.heartbeat.ValidateRequestHandler
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClientUpdateResult
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.ext.mongo.MongoClient
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.handler.BodyHandler
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import rx.Observable
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import java.util.stream.Collectors.toList

/**
 * 处理py机器注册心跳
 */
@Component
class HttpVerticle : AbstractVerticle() {

    private val log = logger(this::class)

    /** 注入全局路由 **/
    @Autowired
    private lateinit var router: Router

    @Autowired
    @Qualifier("config")
    private lateinit var config: JsonObject

    /**
     * 请求校验
     */
    @Autowired
    private lateinit var validateRequestHandler: ValidateRequestHandler

    @Autowired
    private lateinit var mongoClient: MongoClient

    /**
     * 设置3分钟
     */
    private val TIME_DIFF = 1000 * 30

    /**
     * 定时器间隔
     */
    private val TIME_INTERVAL:Long = 1000 * 60

    @Throws(Exception::class)
    override fun start() {
        super.start()
        router.route("/heartbeat/*").handler(BodyHandler.create())
        //请求校验
        router.post("/heartbeat/register").handler(validateRequestHandler)
        //定时处理
        scheduleReFlushServerState()
        vertx.createHttpServer().requestHandler(router).listen(config.value("HTTP.PORT", 8088)) {
            if (it.succeeded()) {
                log.info("Success start HTTP port:${it.result().actualPort()}")
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
            Observable.timer(TIME_INTERVAL, TimeUnit.SECONDS).flatMap {
                println()
                log.info("[定时更新服务器状态(定时器)执行] >>>>>>>>>")
                //查询出正常状态的服务器
                mongoClient.rxFind(Server.tableName,JsonObject().put("status", ServerState.UP.code)).toObservable().map {
                    if(!it.isNullOrEmpty()){
                        //获取3分钟之前的所有并且为可用状态的服务器
                        var timeOutList = it.stream().filter {
                            (System.currentTimeMillis() - it.value<Long>("timestamp", 0)) > TIME_DIFF
                        }.collect(Collectors.toList())
                        if(timeOutList.isNotEmpty()){
                            var hosts = timeOutList.stream().map{ it.value<String>("host") }.collect(toList())
                            log.info("[查询到mongodb超过3分钟未更新的服务] list: $hosts")
                            var list = ArrayList<Observable<MongoClientUpdateResult>>()
                            for(json in timeOutList){
                                list.add(mongoUpdate(json.value("host",""),json.value("port",0)))
                            }
                            Observable.concat(list).toList().toSingle().subscribe({
                                log.info("[设置服务状态下线] hosts : $hosts \n")
                            },{ it.printStackTrace() })
                        }
                    }
                }
            }
        }.repeat().doOnError { it.printStackTrace() }.subscribe()
    }
    /**
     * 更新mongo
     */
    private fun mongoUpdate(host: String,port: Int): Observable<MongoClientUpdateResult> {
        return mongoClient.rxUpdateCollection(
                Server.tableName,
                JsonObject().put("host",host).put("port",port),
                JsonObject().put("\$set",JsonObject().put("status", ServerState.DOWN.code))
        ).toObservable()
    }
}
