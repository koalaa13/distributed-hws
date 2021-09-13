package dijkstra

import dijkstra.messages.*
import dijkstra.system.environment.Environment

class ProcessImpl(private val environment: Environment) : Process {
    private var isInitiator = false
    private var d: Long? = null
    private var balance = 0
    private var childCount = 0
    private var parentId = -1

    private fun updateDist(newDist: Long): Boolean {
        if (d == null) {
            d = newDist
            return true
        }
        if (d!! > newDist) {
            d = newDist
            return true
        }
        return false
    }

    private fun sendDistToNeighbors() {
        for (i in environment.neighbours) {
            if (i.key == environment.pid) continue
            environment.send(i.key, MessageWithDistance(d!! + i.value))
            balance++
        }
    }

    private fun checkFinishExecution() {
        if (balance == 0 && childCount == 0) {
            if (isInitiator) {
                environment.finishExecution()
            } else {
                if (parentId != -1) {
                    environment.send(parentId, DeleteChildMessage)
                    parentId = -1
                }
            }
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        if (message is MessageWithDistance) {
            val srcDist = message.data
            if (updateDist(srcDist)) {
                if (parentId != -1) {
                    environment.send(parentId, DeleteChildMessage)
                }
                parentId = srcId
                environment.send(srcId, AddChildMessage)
                sendDistToNeighbors()
            }
            environment.send(srcId, AckMessage)
        }
        if (message is AckMessage) {
            balance--
            check(balance >= 0)
        }
        if (message is AddChildMessage) {
            childCount++
        }
        if (message is DeleteChildMessage) {
            childCount--
            check(childCount >= 0)
        }
        checkFinishExecution()
    }

    override fun getDistance(): Long? {
        return d
    }

    override fun startComputation() {
        d = 0
        isInitiator = true
        sendDistToNeighbors()
        checkFinishExecution()
    }
}