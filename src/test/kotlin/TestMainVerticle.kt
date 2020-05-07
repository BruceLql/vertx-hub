import fof.daq.hub.Address
import fof.daq.hub.common.value
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonObject
import io.vertx.core.net.NetClient
import java.util.*
import io.vertx.ext.eventbus.bridge.tcp.impl.protocol.FrameHelper
import io.vertx.ext.eventbus.bridge.tcp.impl.protocol.FrameParser
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler


class TestMainVerticle: AbstractVerticle(){

    var socket : NetClient?  = null

    override fun start() {
        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())
        router.route("/cmcc").handler {
            this.socket?.close()
            sockHandler(it) }
        vertx.createHttpServer().requestHandler(router).listen(9090){
            if(it.succeeded()) {
                println("Success start http port:${it.result().actualPort()}")
            } else {
                println("Failed start http")
            }
        }
    }

    private fun sockHandler(content: RoutingContext) {
        val host = content.request().headers().get("host")
        val port = content.request().headers().get("port")?.toInt()
            host ?: content.response().error("服务器连接信息错误").let { return }
            port ?: content.response().error("服务器连接信息错误").let { return }
        val body = content.bodyAsJson
        val uuid = body.value<String>("uuid")
            uuid ?: content.response().error("服务器连接信息错误").let { return }
        // 生成随机MID结果
        val mid = UUID.randomUUID().toString()

        this.socket = vertx.createNetClient().connect(port, host){ ar ->
            val socket = ar.result()
            if (ar.succeeded()) {
                val parser = FrameParser{ ar ->
                    println(">>>>>>> 收到请求：${ar.result()}")
                    if (ar.succeeded()) {
                        val message = ar.result()
                        when(message.getString("type")) {
                            "message" -> {
                                // 回复
                                val body = message.value<JsonObject>("body")
                                val replyAddress =  message.value<String>("replyAddress")
                                when(body.value<String>("ACTION")) {
                                    "CHECK" -> {
                                        if (replyAddress != null) {
                                            println("回复地址：$replyAddress")
                                            FrameHelper.sendFrame("send", replyAddress, JsonObject().put("status", "2"), socket)
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        ar.cause().printStackTrace()
                    }
                }
                socket.handler(parser)
                // 服务注册
                FrameHelper.sendFrame("register", Address.CW.LISTEN + uuid, null, body,true, body, socket)
                // 模拟验证
                vertx.setTimer(10 * 1000){
                    val headers = body.put("mid", mid).put("timeout",50 * 1000)
                    val message = JsonObject().put("img", "base64image").put("code", "111")
                    println("模拟10秒后发送验证请求$message")
                    FrameHelper.sendFrame("send", Address.WEB.PROXY, UUID.randomUUID().toString(), headers,true, message, socket)
                }
                vertx.setTimer(12 * 1000){
                    val headers = body.put("mid", mid).put("timeout",50 * 1000)
                    val message = JsonObject().put("img", "base64image").put("code", "222")
                    println("模拟10秒后发送验证请求$message")
                    FrameHelper.sendFrame("send", Address.WEB.PROXY, UUID.randomUUID().toString(), headers,true, message, socket)
                }
            } else {
                ar.cause().printStackTrace()
            }
        }
        val result = JsonObject().put("mid", mid)
        content.response().putHeader("Content-Type", "application/json; charset=utf-8").end(result.encodePrettily())
    }

    fun HttpServerResponse.error(error: String) {
        this.setStatusCode(500).success(JsonObject().put("error", error))
    }

    fun HttpServerResponse.success(data:JsonObject) {
        this.putHeader("Content-Type", "application/json; charset=utf-8")
            .end(data.encodePrettily())
    }

    companion object {
        @JvmStatic
        fun main(args:Array<String>) {
            // 初始化类
            Vertx.vertx().deployVerticle(TestMainVerticle())
        }
    }
}