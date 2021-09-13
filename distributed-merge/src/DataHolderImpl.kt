import system.DataHolderEnvironment

class DataHolderImpl<T : Comparable<T>>(
    private val keys: List<T>,
    private val dataHolderEnvironment: DataHolderEnvironment
) : DataHolder<T> {
    private var curIndex = 0
    private var lastCheckpointIndex = 0

    private fun elemsLeft(): Int {
        return keys.size - curIndex
    }

    override fun checkpoint() {
        lastCheckpointIndex = curIndex
    }

    override fun rollBack() {
        curIndex = lastCheckpointIndex
    }

    override fun getBatch(): List<T> {
        val batchSize = dataHolderEnvironment.batchSize
        return if (elemsLeft() < batchSize) {
            val res = keys.subList(curIndex, keys.size)
            curIndex = keys.size
            res
        } else {
            curIndex += batchSize
            keys.subList(curIndex - batchSize, curIndex)
        }
    }
}