package fof.daq.hub.web.handler.heartbeat

import fof.daq.hub.common.enums.HttpStatus
import fof.daq.hub.common.enums.ServerState
import fof.daq.hub.common.error
import fof.daq.hub.common.logger
import fof.daq.hub.common.success
import fof.daq.hub.common.utils.MD5
import fof.daq.hub.common.value
import fof.daq.hub.model.Server
import fof.daq.hub.service.HeartBeatService
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.rxjava.ext.web.RoutingContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller

/**
 * 验证请求参数
 */
@Controller
class ValidateRequestHandler : Handler<RoutingContext> {

    private val log = logger(this::class)

    //MD5签名 key
    private val MD5_SIGNKEY = "MTkyLjE2OC4xLjEzMzo4MDgwMTkyLjE2OC4xLjEzMzo4MDgw"

    @Autowired
    private lateinit var heartBeatService: HeartBeatService



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

        val tags = body.value<JsonArray>("tags")
        tags ?: event.response().error(HttpStatus.BAD_REQUEST, "tags is required param").let { return }

        val sign = body.value<String>("sign")
        sign ?: event.response().error(HttpStatus.BAD_REQUEST, "sign is required param").let { return }
        //验证签名是否合法 host + port + serverName + timestamp + version + tags +  MD5_SIGNKEY
        val strSign = MD5.encryption(host + port + serverName + timestamp + version + tags.toString() + MD5_SIGNKEY)
        if (!strSign.equals(sign)) {
            log.error("[校验心跳注册请求] error : sign is failed")
            event.response().error(HttpStatus.BAD_REQUEST, "sign is failed")
            return
        }
        //封装server
        val server = Server(host = host, port = port, server_name = serverName, timestamp = timestamp, version = version, tags = Server.calcTags(tags), status = ServerState.UP.code, switch = 1, created_at = timestamp)
        //心跳注册处理
        heartBeat(server)
        event.response().success()
    }
    /**
     * 注册处理
     */
    private fun heartBeat(server: Server) {
        heartBeatService.findOneByHostOrPort(server.host, server.port)
                .doOnError { it.printStackTrace() }
                .subscribe {
                    log.info("[注册前查询mongodb] 是否存在: ${!it.isNullOrEmpty()}")
                    //不存在插入数据库
                    //否则更新
                    when (it.isNullOrEmpty()) {
                        true -> heartBeatService.save(server)
                        else -> heartBeatService.updateByStatusOrTime(server.host, server.port, ServerState.UP, server.timestamp,server.tags).subscribe()
                    }
        }
    }
}
