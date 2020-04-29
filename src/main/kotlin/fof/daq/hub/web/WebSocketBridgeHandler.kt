package fof.daq.hub.web

import fof.daq.hub.common.logger
import fof.daq.hub.web.chain.ConnectChain
import fof.daq.hub.web.chain.RegisterChain
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer.buffer
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.handler.sockjs.BridgeEvent
import io.vertx.ext.web.handler.sockjs.SockJSSocket
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import rx.Observable

/**
 * Sock 桥接访问处理控制器
 * */
@Controller
class WebSocketBridgeHandler @Autowired constructor(
        private val connectChain: ConnectChain, // 连接链
        private val registerChain: RegisterChain // WEB请求注册服务
): Handler<BridgeEvent> {
    private val log = logger(this::class)
    override fun handle(event: BridgeEvent) {
        Observable.just(event)
                  .flatMap(connectChain::bridge)
                  .flatMap(registerChain::bridge)
                  .subscribe({
                      it.complete(true) // 默认数据处理完成
                  },{
                      /*返回错误信息并中断事件*/
                      replyError(event.socket(), it)
                      event.fail(it)
                      log.error(it)
                  })
    }

    companion object {
        /**
         * 返回错误信息
         * */
        fun replyError(sock: SockJSSocket, err: Throwable) {
            val envelope = JsonObject().put("type", "err").put("body", err.message)
            sock.write(buffer(envelope.encode()))
        }
    }
}