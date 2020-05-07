package fof.daq.hub.web.handler

import fof.daq.hub.Address
import fof.daq.hub.common.utils.LogUtils
import fof.daq.hub.component.CrawlerServer
import fof.daq.hub.model.Customer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.eventbus.EventBus
import io.vertx.rxjava.core.eventbus.Message
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller

/**
 * 初始化注册采集服务器
 * */
@Controller
class HubProxyHandler @Autowired constructor(
        private val crawlerServer: CrawlerServer,
        private val eb: EventBus
) : AbstractConsumerHandler() {
    private val logUtils = LogUtils(this::class.java)
    override fun consumer(customer: Customer?, message: Message<JsonObject>) {
        if (customer == null) {
            log.error(NullPointerException("[HubProxy] Customer is null"))
            return
        }
        val mid = customer.mid
        if (mid == null) {
            logUtils.error(customer.mobile, "[消息代理-错误] 参数缺失", NullPointerException("Mid is null"))
            return
        }
        val address = Address.WEB.listen(mid)
        val body = message.body()
        val timeout:Long = message.headers().get("timeout")?.toLong() ?: 20 * 1000
        val option = DeliveryOptions().setSendTimeout(timeout)
            option.headers = message.headers().delegate
        logUtils.info(customer.mobile, "[消息代理-请求] 地址:$address / Headers: ${message.headers().toList()} / Body:${message.body()}", customer)
        // todo NoHandler 重试
        // todo 接收Crawler 关闭通知 重分配采集服务
        eb.rxSend<JsonObject>(address, body, option)
          .subscribe({ msg ->
              message.reply(msg.body(), DeliveryOptions().setSendTimeout(10 * 1000))
              logUtils.info(customer.mobile, "[消息代理-回复] 地址:$address / Result:${msg.body()}", customer)
          },{ err ->
              message.fail(1, err.message ?: "Unknown exception error")
              logUtils.error(customer.mobile, "[消息代理-失败]", err, customer)
          })
    }
}
