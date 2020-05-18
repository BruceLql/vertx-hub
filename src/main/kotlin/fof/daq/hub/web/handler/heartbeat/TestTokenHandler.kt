package fof.daq.hub.web.handler.heartbeat

import fof.daq.hub.common.logger
import fof.daq.hub.common.success
import fof.daq.hub.common.utils.MD5
import fof.daq.hub.common.value
import io.vertx.core.Handler
import io.vertx.rxjava.ext.web.RoutingContext
import org.springframework.stereotype.Controller
import vts.jwt.JWTAuth
import vts.jwt.json.JsonObject


/**
 * 测试生成token
 */
@Controller
class TestTokenHandler : Handler<RoutingContext> {

    private val log = logger(this::class)

    override fun handle(event: RoutingContext) {
        val body = event.bodyAsJson
        log.info("[测试生成token] body : $body")
        val mobile = body.value<String>("mobile")?: throw NullPointerException("Mobile is null").let { return }
        val isp = body.value<String>("isp")?: throw NullPointerException("Isp is null").let { return }
        val userId = body.value<String>("userId")?: throw NullPointerException("UserId is null").let { return }
        val callBack = body.value<String>("callBack")?: throw NullPointerException("CallBack is null").let { return }
        val name = body.value<String>("name")?: throw NullPointerException("Name is null").let { return }
        val cid = body.value<String>("cid")?: throw NullPointerException("Cid is null").let { return }
        val notifyUrl = body.value<String>("notifyUrl")?: throw NullPointerException("NotifyUrl is null").let { return }
        val nonce = body.value<Number>("nonce")?: throw NullPointerException("Nonce is null").let { return }

        var jwtAuth = JWTAuth.create(JsonObject()
                .put("keyStore", JsonObject()
                        .put("type", "jceks") // 签名文件类型
                        .put("path", "./src/main/resources/keystore.jceks") // 签名测试文件
                        .put("password", "secret")))
        val data = JsonObject()
                .put("mobile", mobile)
                .put("isp", isp)
                .put("userId", userId)
                .put("callBack", callBack)
                .put("name", name)
                .put("cid", cid)
                .put("notifyUrl", notifyUrl)
                .put("nonce", nonce)
        val token = jwtAuth.generateToken(data)
        val uuid = MD5.digest(token)
        log.info("[测试生成token] uuid : $uuid")
        event.response().success(io.vertx.core.json.JsonObject().put("token",token))
    }
}


