package fof.daq.hub.web

import io.vertx.core.Handler
import io.vertx.rxjava.ext.web.RoutingContext
import org.springframework.stereotype.Controller

/**
 * 权限验证,提取URL的TOKEN参数转换至HEADER
 */
@Controller
class WebSocketAuthHandler : Handler<RoutingContext> {
    override fun handle(routingContext: RoutingContext) {
        val token = routingContext.request().getParam("token")
        if (token != null) {
            routingContext.request().headers().set("Authorization", "Bearer $token")
        }
        routingContext.next()
    }
}
