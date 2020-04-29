package fof.daq.hub.common.utils

import fof.daq.hub.common.logger
import rx.Observable
import rx.functions.Func1

import java.util.concurrent.TimeoutException

class RetryWithTimeOut(private val maxRetries: Int) : Func1<Observable<out Throwable>, Observable<*>> {
    private val log = logger(this::class)
    private var retryCount: Int = 0

    override fun call(attempts: Observable<out Throwable>): Observable<*> {
        return attempts.flatMap { throwable ->
            if (throwable is TimeoutException && ++retryCount <= maxRetries) {
                log.warn("TimeoutException and retry: $retryCount")
                Observable.just(null)
            } else {
                Observable.error(throwable)
            }
        }
    }
}