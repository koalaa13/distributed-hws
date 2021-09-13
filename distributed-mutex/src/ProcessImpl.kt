package mutex

/**
 * Distributed mutual exclusion implementation.
 * All functions are called from the single main thread.
 *
 * @author Nikita Maksimov
 */
class ProcessImpl(private val env: Environment) : Process {
    private enum class ForkStatus { CLEAN, DIRTY, ANOTHER }
    private enum class MsgType { REQ, OK }

    private var inCS = false
    private var wantCS = false
    private var needForks = 0
    private val forks = Array(env.nProcesses + 1) { ForkStatus.ANOTHER }
    private val pendingOk = BooleanArray(env.nProcesses + 1)

    init {
        for (i in env.processId..env.nProcesses) {
            forks[i] = ForkStatus.DIRTY
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        message.parse {
            when (readEnum<MsgType>()) {
                MsgType.OK -> processOk(srcId)
                MsgType.REQ -> processReq(srcId)
            }
        }
    }

    private fun processOk(srcId: Int) {
        check(wantCS) { "I never asked for this OK" }
        forks[srcId] = ForkStatus.CLEAN
        needForks--
        checkCSEnter()
    }

    private fun processReq(srcId: Int) {
        when (forks[srcId]) {
            ForkStatus.ANOTHER -> throw IllegalStateException("Process $srcId asked for taken fork")
            ForkStatus.CLEAN -> {
                check(!pendingOk[srcId]) { "Process $srcId already asked for this fork" }
                if (wantCS || inCS) {
                    pendingOk[srcId] = true
                } else {
                    forks[srcId] = ForkStatus.ANOTHER
                    send(srcId, MsgType.OK)
                }
            }
            ForkStatus.DIRTY -> {
                check(!pendingOk[srcId]) { "Process $srcId already asked for this fork" }
                if (inCS) {
                    pendingOk[srcId] = true
                } else {
                    forks[srcId] = ForkStatus.ANOTHER
                    send(srcId, MsgType.OK)
                    if (wantCS) {
                        needForks++
                        send(srcId, MsgType.REQ)
                    }
                }
            }
        }
    }

    override fun onLockRequest() {
        check(!inCS && !wantCS) { "Lock was already requested" }
        wantCS = true
        for (i in 1..env.nProcesses) {
            if (i != env.processId) {
                if (forks[i] == ForkStatus.ANOTHER) {
                    send(i, MsgType.REQ)
                    needForks++
                }
            }
        }
        checkCSEnter()
    }

    override fun onUnlockRequest() {
        check(inCS) { "Can not unlock because we are not in CS" }
        inCS = false
        env.unlocked()
        for (i in 1..env.nProcesses) {
            if (i != env.processId) {
                if (pendingOk[i]) {
                    pendingOk[i] = false
                    forks[i] = ForkStatus.ANOTHER
                    send(i, MsgType.OK)
                } else {
                    forks[i] = ForkStatus.DIRTY
                }
            }
        }

    }

    private fun checkCSEnter() {
        if (needForks == 0) {
            wantCS = false
            inCS = true
            env.locked()
        }
    }

    private fun send(destId: Int, type: MsgType, builder: MessageBuilder.() -> Unit = {}) {
        env.send(destId) {
            writeEnum(type)
            builder()
        }
    }
}
