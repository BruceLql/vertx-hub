package fof.daq.hub.web.chain

import fof.daq.hub.component.CrawlerServer
import io.vertx.ext.bridge.BridgeEventType
import io.vertx.ext.web.handler.sockjs.BridgeEvent
import io.vertx.rxjava.core.Vertx
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import rx.Observable

/**
 * 注册链
 * */
@Controller
class RegisterChain @Autowired constructor(
        vertx: Vertx, crawlerServer: CrawlerServer
): AbstractChain(vertx, crawlerServer){
    override fun bridge(event: BridgeEvent): Observable<BridgeEvent>
    = when(event.type()) {
        BridgeEventType.SEND -> this.reHeaders(event)
        else -> Observable.just(event)
    }

}