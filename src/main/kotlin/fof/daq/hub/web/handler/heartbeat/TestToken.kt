package fof.daq.hub.web.handler.heartbeat

import io.vertx.core.Handler
import io.vertx.ext.auth.jwt.JWTAuth
import io.vertx.rxjava.ext.web.RoutingContext
import org.springframework.stereotype.Controller


/**
 * 测试生成token
 */
@Controller
class TestToken : Handler<RoutingContext> {



    override fun handle(event: RoutingContext) {
        val mobile = event.request().getParam("mobile")
        val isp = event.request().getParam("isp")


    }
}


