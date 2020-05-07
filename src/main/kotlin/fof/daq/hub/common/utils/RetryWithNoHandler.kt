package fof.daq.hub.common.utils

import fof.daq.hub.common.logger
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.eventbus.ReplyFailure
import rx.Observable
import rx.functions.Func1
import java.util.concurrent.TimeUnit

class RetryWithNoHandler(private val delay:Long = 1) : Func1<Observable<out Throwable>, Observable<*>> {
    private val log = logger(this::class)
    private var retryCount: Int = 0

    override fun call(attempts: Observable<out Throwable>): Observable<*> {
        return attempts.flatMap { throwable ->
            if (throwable is ReplyException && throwable.failureType() == ReplyFailure.NO_HANDLERS) {
                ++retryCount
                log.warn("[ReplyException] NoHandler delay ${delay}s to retry: $retryCount")
                Observable.timer(delay, TimeUnit.SECONDS)
            } else {
                Observable.error(throwable)
            }
        }
    }
}