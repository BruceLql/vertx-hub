package fof.daq.hub.web.handler

import fof.daq.hub.Address
import fof.daq.hub.common.logger
import fof.daq.hub.common.value
import fof.daq.hub.component.CrawlerServer
import fof.daq.hub.model.Customer
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.MultiMap
import io.vertx.rxjava.core.eventbus.EventBus
import io.vertx.rxjava.core.eventbus.Message
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import rx.Single
/**
 * 初始化注册采集服务器
 * */
@Controller
class InitCrawlerHandler @Autowired constructor(
        private val crawlerServer: CrawlerServer,
        private val eb: EventBus
) : AbstractConsumerHandler() {

    override fun consumer(customer: Customer? , message: Message<JsonObject>) {
        // 获取经过封装的客户信息
        if (customer == null){
            message.fail(0, "Customer is null")
            return
        }
        val body =  message.body()
        body.value<String>("mobile")?.also { customer.mobile = it }
        body.value<String>("isp")?.also { customer.isp = it }

        // 先取消原有加载程序
        listInitObservable[customer.uuid]?.unsubscribe()
        // 注册或获取采集服务器记录
        listInitObservable[customer.uuid] = crawlerServer.server(customer.uuid){ am, oldCustomer ->
            if (oldCustomer == null) {
                buildCrawler(customer)
                        .flatMap { _customer -> // 保存至集群服务器
                            am.rxPut(_customer.uuid, _customer.toJson()).map { _customer }
                        }
            } else {
                if (oldCustomer.mobile != customer.mobile || oldCustomer.isp != customer.isp) {
                    updateCrawlerServer(oldCustomer, customer)
                            .flatMap { _customer ->
                                am.rxReplace(oldCustomer.uuid, _customer.toJson()).map { _customer }
                            }
                } else {
                    checkCrawlerServer(oldCustomer).map { oldCustomer } // 校验检查服务是否运行中
                }
            }
        }
        .doAfterTerminate { listInitObservable.remove(customer.uuid) }
        .subscribe({
            message.reply(it?.toJson() ?: JsonObject())
        },{
            log.error(it)
            message.fail(1, it.message)
        })
    }

    /**
     * 通知原有采集服务终止关闭
     * 重选服务 TODO 顺序需要调整优化
     * */
    private fun updateCrawlerServer(oldCustomer: Customer, newCustomer: Customer): Single<Customer> {
        return this.eb.rxSend<JsonObject>(Address.CW.listen(oldCustomer.uuid), Address.CW.action(Address.Action.STOP, oldCustomer.toJson())) // 发送通知关闭服务
              .flatMap { this.buildCrawler(newCustomer) } // 重新选举开启服务  TODO 可能存在同时同一个机器关闭和开启，PY需要处理判断ISP
    }


    /**
     * 验证服务是否正在运行
     * */
    private fun checkCrawlerServer(oldCustomer: Customer): Single<JsonObject> {
        log.info("Check Crawler server: ${oldCustomer.uuid}")
        // 请求事件为检查
        return eb.rxSend<JsonObject>(Address.CW.listen(oldCustomer.uuid), Address.CW.action(Address.Action.CHECK, oldCustomer.toJson()))
               .map { it.body() } // TODO 验证返回结果是否符合
    }


    /**
     * 选择采集服务器
     * */
    private fun buildCrawler(customer: Customer): Single<Customer> {
        val params = customer.toJson()
        val headers = MultiMap.caseInsensitiveMultiMap()
            headers.add("host", "127.0.0.1")
            headers.add("port", "8080")
        return crawlerServer.register(params, headers)
                .map {
                    log.info("[Build Crawler] UUID: ${customer.uuid} to MID: ${it.first}")
                    // it.second //TODO 根据需要处理返回结果，例如保存服务器IP等信息
                    customer.apply { this.mid = it.first }
                }.toSingle()
    }

}
