package fof.daq.hub.web.chain

import fof.daq.hub.component.CrawlerServer
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.handler.sockjs.BridgeEvent
import io.vertx.rxjava.core.Vertx
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import rx.Observable
import io.vertx.ext.bridge.BridgeEventType.SOCKET_CREATED
import io.vertx.ext.bridge.BridgeEventType.SOCKET_CLOSED
/**
 * 连接链
 * */
@Controller
class ConnectChain @Autowired constructor(
        vertx: Vertx, crawlerServer: CrawlerServer
): AbstractChain(vertx, crawlerServer){
    override fun bridge(event: BridgeEvent): Observable<BridgeEvent> {
        log.debug("[SOCK-JS] Type: ${event.type()}${event.rawMessage?.let { " / raw: $it" } ?: ""}")
        if (event.type() ==  SOCKET_CREATED || event.type() == SOCKET_CLOSED) {
            try {
                event.socket()?.webSession()?.get<JsonObject>(SESSION_CUSTOMER)?.also {
                    when(event.type()) {
                        SOCKET_CREATED -> this.sockCreate(it)
                        SOCKET_CLOSED -> this.sockClose(it)
                    }
                }
            } catch (e: Exception) {
                log.error(e)
            }
        }
        return Observable.just(event)
    }
}