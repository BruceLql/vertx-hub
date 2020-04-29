package fof.daq.hub.common.exception

class NoSuchServerException : RuntimeException {
    constructor() : super()
    constructor(s: String) : super(s)
}
