package fof.daq.hub.web.handler.heartbeat

import fof.daq.hub.common.enums.HttpStatus
import fof.daq.hub.common.enums.ServerState
import fof.daq.hub.common.error
import fof.daq.hub.common.logger
import fof.daq.hub.common.success
import fof.daq.hub.common.utils.MD5
import fof.daq.hub.common.value
import fof.daq.hub.model.Server
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClientUpdateResult
import io.vertx.rxjava.ext.mongo.MongoClient
import io.vertx.rxjava.ext.web.RoutingContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import rx.Observable

/**
 * 验证请求参数
 */
@Controller
class ValidateRequestHandler : Handler<RoutingContext> {

    private val log = logger(this::class)

    //MD5签名 key
    private val MD5_SIGNKEY = "MTkyLjE2OC4xLjEzMzo4MDgwMTkyLjE2OC4xLjEzMzo4MDgw"

    @Autowired
    private lateinit var mongoClient: MongoClient


    override fun handle(event: RoutingContext) {
        var body = event.bodyAsJson
        println()
        log.info("[校验心跳注册请求] body : $body")
        body ?: event.response().error(HttpStatus.BAD_REQUEST).let {
            log.error("[验证注册心跳请求] error : body is null")
            return
        }

        val host = body.value<String>("host")
        host ?: event.response().error(HttpStatus.BAD_REQUEST, "host is required param").let { return }

        val port = body.value<Int>("port")
        port ?: event.response().error(HttpStatus.BAD_REQUEST, "port is required param").let { return }

        val serverName = body.value<String>("serverName")
        serverName ?: event.response().error(HttpStatus.BAD_REQUEST, "serverName is required param").let { return }

        val timestamp = body.value<Long>("timestamp")
        timestamp ?: event.response().error(HttpStatus.BAD_REQUEST, "timestamp is required param").let { return }

        val version = body.value<String>("version")
        version ?: event.response().error(HttpStatus.BAD_REQUEST, "version is required param").let { return }

        val sign = body.value<String>("sign")
        sign ?: event.response().error(HttpStatus.BAD_REQUEST, "sign is required param").let { return }
        //验证签名是否合法 host + port + serverName + timestamp + version + MD5_SIGNKEY
        val strSign = MD5.encryption(host + port + serverName + timestamp + version + MD5_SIGNKEY)
        log.info("[测试打印sign] sign :$strSign")
        if (!strSign.equals(sign)) {
            log.error("[校验心跳注册请求] error : sign is failed \n")
            event.response().error(HttpStatus.BAD_REQUEST, "sign is failed")
            return
        }
        //标签
        val tags = body.value<JsonArray>("tags", JsonArray())
        //封装server
        val server = Server(host = host, port = port, server_name = serverName, timestamp = timestamp, version = version, tags = tags, status = ServerState.UP.code, switch = 0, created_at = timestamp)
        //心跳注册处理
        heartBeat(server)
        event.response().success()
    }

    /**
     * 注册处理
     */
    private fun heartBeat(server: Server) {
        //先查询mongo是否存在
        validateMongo(server.host, server.port).subscribe({
            log.info("[注册前查询mongodb] 是否存在: $it")
            //不存在录入数据库
            if (it == null) {
                //录入数据库
                saveToMongo(server).subscribe({ log.info("[注册前查询mongodb 并存入数据库] \n") }, { it.printStackTrace() })
            } else {
                //更新服务器信息
                updateMongo(server.host, server.port, server.timestamp).subscribe({ log.info("[注册前查询mongodb 得到结果为已存在，并更新服务(timestamp,status)] \n") }, { it.printStackTrace() })
            }
        }, { it.printStackTrace() })
    }

    /**
     * 验证mongo 中是否存在
     */
    private fun validateMongo(host: String, port: Int): Observable<JsonObject> {
        return mongoClient.rxFindOne(
                Server.tableName,
                JsonObject().put("host", host).put("port", port),
                JsonObject()
        ).toObservable()
    }

    /**
     * 存入mongo
     */
    private fun saveToMongo(server: Server): Observable<String> {
        return mongoClient.rxInsert(Server.tableName, server.toJson()).toObservable()
    }

    /**
     * 更新mongo
     */
    private fun updateMongo(host: String, port: Int, timestamp: Long): Observable<MongoClientUpdateResult> {
        return mongoClient.rxUpdateCollection(
                Server.tableName,
                JsonObject().put("host", host).put("port", port),
                JsonObject()
                        .put("\$set",
                                JsonObject().put("timestamp", timestamp)
                                .put("status",ServerState.UP.code)
                        )
        ).toObservable()
    }
}
