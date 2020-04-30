import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonObject
import java.net.Socket
import java.util.*
import io.vertx.ext.eventbus.bridge.tcp.impl.protocol.FrameHelper
import io.vertx.ext.eventbus.bridge.tcp.impl.protocol.FrameParser


class TestMainVerticle: AbstractVerticle(){
    override fun start() {
        vertx.createHttpServer().requestHandler {
            when(it.uri()) {
                "/cmcc" -> sockHandler(it)
                else -> it.response().end("No Handler for this uri")
            }
        }.listen(9090){
            if(it.succeeded()) {
                println("Success start http port:${it.result().actualPort()}")
            } else {
                println("Failed start http")
            }
        }
    }

    private fun sockHandler(content: HttpServerRequest) {
        val host = content.headers().get("host")
        val port = content.headers().get("port")?.toInt()
            host ?: content.response().error("服务器连接信息错误")
            port ?: content.response().error("服务器连接信息错误").let { return }

        val parser = FrameParser{ ar ->
            if (ar.succeeded()) {
                val message = ar.result()
                when(message.getString("type")) {
                    "message" -> {}
                }
            } else {
                ar.cause().printStackTrace()
            }
        }
        vertx.createNetClient().connect(port, host){ ar ->
            val socket = ar.result()
            if (ar.succeeded()) {
                socket.handler(parser)
                FrameHelper.sendFrame("register", "FOF.TEST", JsonObject().put("aaa", "bbb"), socket)
            } else {
                ar.cause().printStackTrace()
            }
        }

        // 生成随机MID结果
        val result = JsonObject().put("mid", UUID.randomUUID().toString())
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