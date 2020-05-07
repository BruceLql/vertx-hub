package fof.daq.hub.web.handler

import fof.daq.hub.Address
import fof.daq.hub.common.utils.LogUtils
import fof.daq.hub.common.utils.RetryWithNoHandler
import fof.daq.hub.component.CrawlerServer
import fof.daq.hub.model.Customer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.eventbus.EventBus
import io.vertx.rxjava.core.eventbus.Message
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import java.util.concurrent.TimeUnit

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
        // todo 接收Crawler 关闭通知 重分配采集服务

        val timeout = message.headers().get("timeout")?.toLong() ?: 20 * 1000 // 前端默认等待超时时间
        val option = DeliveryOptions().setSendTimeout(timeout).setHeaders(message.headers().delegate)
        logUtils.info(customer.mobile, "[消息代理] 请求地址:$address / Headers: ${message.headers().toList()} / Body:${message.body()}", customer)
        eb.rxSend<JsonObject>(address, body, option)
          .retryWhen(RetryWithNoHandler(1)) // 无地址重试等待间隔1秒
          .timeout(timeout, TimeUnit.MILLISECONDS) // 请求超时时间
          .subscribe({ msg ->
              val replyOption = DeliveryOptions().setSendTimeout(20 * 1000)
              logUtils.info(customer.mobile, "[消息代理] 回复地址:${message.replyAddress()} / Result:${msg.body()}", customer)
              message.replyAndRequest<JsonObject>(msg.body(), replyOption){ ar ->
                  if (ar.succeeded()) {
                      msg.reply(JsonObject().put("status", "SUCCESS").put("message", "消息回复成功").put("result", ar.result().body()))
                      logUtils.trace(customer.mobile, "[消息代理] 回复成功 Body:${ar.result().body()}", customer)
                  } else {
                      msg.reply(JsonObject().put("status", "FAILED").put("message", "消息已回复，无返回信息").put("reason", ar.cause().message))
                      logUtils.failed(customer.mobile, "[消息代理] 回复失败", ar.cause(), customer)
                  }
              }
          },{ err ->
              message.fail(1, err.message ?: "Unknown exception error")
              logUtils.error(customer.mobile, "[消息代理] 错误", err, customer)
          })
    }
}
