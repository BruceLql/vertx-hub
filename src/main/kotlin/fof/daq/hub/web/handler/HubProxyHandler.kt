package fof.daq.hub.web.handler

import fof.daq.hub.component.CrawlerServer
import fof.daq.hub.model.Customer
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.eventbus.Message
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller

/**
 * 初始化注册采集服务器
 * */
@Controller
class HubProxyHandler @Autowired constructor(
        private val crawlerServer: CrawlerServer
) : AbstractConsumerHandler() {
    override fun consumer(customer: Customer?, message: Message<JsonObject>) {

    }

}
