package fof.daq.hub.web.handler

import fof.daq.hub.Address
import fof.daq.hub.common.utils.LogUtils
import fof.daq.hub.common.utils.RetryWithNoHandler
import fof.daq.hub.common.value
import fof.daq.hub.component.CrawlerServer
import fof.daq.hub.model.Customer
import fof.daq.hub.service.CollectNoticeService
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.eventbus.EventBus
import io.vertx.rxjava.core.eventbus.Message
import io.vertx.rxjava.ext.web.client.WebClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import rx.Observable
import java.util.concurrent.TimeUnit

/**
 * 初始化注册采集服务器
 * */
@Controller
class HubProxyHandler @Autowired constructor(
        private val crawlerServer: CrawlerServer,
        private val eb: EventBus,
        private val webClient: WebClient,
        private val config: JsonObject,
        private val initCrawlerHandler: InitCrawlerHandler,
        private val collectNoticeService: CollectNoticeService
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
        logUtils.info(customer.mobile, "[接收到代理请求] body:$body", customer)
        //接收Crawler 关闭通知 重分配采集服务
        when (body.value<String>("EVENT")) {
            //完成之后，调用数据清洗服务接口
            Address.Event.DONE.name -> {
                eventDoneMessage(address, mid, customer, body, message)
            }
            //停止，重新选择CW服务器
            Address.Event.STOP.name -> {
                eventStopMessage(address, mid, customer, body, message)
            }
            //如果是timeout 事件清空sd内容
            Address.Event.TIMEOUT.name -> {
                logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:TIMEOUT")
                //清空sd
                crawlerServer.clearPyServerByUuid(customer.uuid)
                        .doOnError {
                            it.printStackTrace()
                            log.error("[接收到代理请求] EVENT:TIMEOUT] 异常清空sd内容")
                        }
                        .subscribe()
                log.info("[接收到代理请求] EVENT:TIMEOUT] 成功清空sd内容")
            }
            else -> messageProxy(body, address, customer, message)
        }
    }


    /**
     * 收到py done事件处理
     */
    private fun eventDoneMessage(address: String, mid: String, customer: Customer, body: JsonObject, message: Message<JsonObject>) {
        logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:DONE")
        //回复py已接收完成通知
        message.reply(JsonObject().put("msg", "[已接收 EVENT:DONE 事件]"))
        Observable.zip(
                collectNoticeService.findOneByUuid(customer.uuid).toObservable(),
                collectNoticeService.update(customer.uuid, mid).toObservable()
        ) { b, c ->
            log.info("[接收到代理请求] EVENT:DONE 更新mid到db中以便清洗数据获取 ${c.toJson()}")
            //再将消息代理到前端 （加入notifyUrl）一并返回
            body.put("notifyUrl", b.rows[0].value("notify_url", ""))
            messageProxy(body, address, customer, message)
        }
                .doOnError { it.printStackTrace() }
                .subscribe()
    }

    /**
     * 收到py stop事件处理，更换节点，并通知前端更新mid
     */
    private fun eventStopMessage(address: String, mid: String, customer: Customer, body: JsonObject, message: Message<JsonObject>){
        logUtils.trace(customer.mobile, "[接收到代理请求] EVENT:STOP")
        initCrawlerHandler.buildCrawler(customer)
                .doOnError { error ->
                    logUtils.failed(customer.mobile, "[接收到代理请求] EVENT:STOP 重新分配服务器失败", error)
                }
                .doOnSuccess {
                    //回复已经重新分配服务
                    message.reply(JsonObject().put("code", 200).put("msg", "服务器已重新分配"))
                    logUtils.info(customer.mobile, "[已重新分配py服务]")
                    //给前端发送 update mid 事件
                    var event = Address.WEB.event(Address.Event.UPDATE).put("mid", customer.mid)
                    eb.send<JsonObject>(address, event) { ar ->
                        if (ar.succeeded()) {
                            logUtils.trace(customer.mobile, "[成功向前端发送更新mid事件]")
                        } else {
                            logUtils.failed(customer.mobile, "[通知前端更新mid事件失败]", ar.cause())
                        }
                    }
                }.subscribe()
    }




    /**
     * 消息代理
     */
    private fun messageProxy(body: JsonObject, address: String, customer: Customer, message: Message<JsonObject>) {
        val timeout = message.headers().get("timeout")?.toLong() ?: 20 * 1000 // 前端默认等待超时时间
        val option = DeliveryOptions().setSendTimeout(timeout).setHeaders(message.headers().delegate)
        logUtils.info(customer.mobile, "[CW代理] 请求地址:$address / Headers: ${message.headers().toList()} / Body:${message.body()}", customer)
        eb.rxSend<JsonObject>(address, body, option)
                .retryWhen(RetryWithNoHandler(1)) // 无地址重试等待间隔1秒
                .timeout(timeout, TimeUnit.MILLISECONDS) // 请求超时时间
                .subscribe({ msg ->
                    val replyOption = DeliveryOptions().setSendTimeout(20 * 1000)
                    logUtils.info(customer.mobile, "[CW代理] 回复地址:${message.replyAddress()} / Result:${msg.body()}", customer)
                    message.replyAndRequest<JsonObject>(msg.body(), replyOption) { ar ->
                        if (ar.succeeded()) {
                            msg.reply(JsonObject().put("status", "SUCCESS").put("message", "消息回复成功").put("result", ar.result().body()))
                            logUtils.trace(customer.mobile, "[CW代理] 回复成功 Body:${ar.result().body()}", customer)
                        } else {
                            msg.reply(JsonObject().put("status", "FAILED").put("message", "消息已回复，无返回信息").put("reason", ar.cause().message))
                            logUtils.failed(customer.mobile, "[CW代理] 回复失败", ar.cause(), customer)
                        }
                    }
                }, { err ->
                    message.fail(1, err.message ?: "Unknown exception error")
                    logUtils.error(customer.mobile, "[CW代理] 错误", err, customer)
                })
    }

    /**
     * 调用数据清洗服务
     */
    private fun collectNotice(customer: Customer): Observable<String> {
        logUtils.info(customer.mobile, "[调用数据清洗服务]", customer)
        val url = config.value<String>("COLLECT.NOTICE")
        if (url == null) log.error(NullPointerException("url is null"))
        val mid = customer.mid
        if (mid == null) {
            logUtils.error(customer.mobile, "[参数校验-错误] 参数缺失", NullPointerException("Mid is null"))
            return Observable.error(NullPointerException("Mid is null"))
        }
        var requestBody = JsonObject()
                .put("task_id", customer.mid)
        // todo 加一个类型参数通知李启岚区分是否在于缓存内

        return this.webClient.postAbs(url).rxSendJsonObject(requestBody)
                .toObservable()
                .doOnNext { res ->
                    when (res.statusCode()) {
                        200 -> logUtils.info(customer.mobile, "[调用数据清洗服务已完成] ${res.bodyAsString()}")
                        else -> logUtils.error(customer.mobile, "[调用数据清洗服务失败！]", Exception("调用请求返回非200"))
                    }
                }.map { it.bodyAsString() }
                .doOnError { it.printStackTrace() }
                .retryWhen(RetryWithNoHandler(1))
    }
}
