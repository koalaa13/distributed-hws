import system.MergerEnvironment
import java.util.*

class MergerImpl<T : Comparable<T>>(
    private val mergerEnvironment: MergerEnvironment<T>,
    prevStepBatches: Map<Int, List<T>>?
) : Merger<T> {
    private val pairComparator = compareBy<Pair<T, Int>> { it.first }.thenBy { it.second }
    private val h = sortedSetOf(pairComparator)
    private val batches: MutableMap<Int, List<T>> = mutableMapOf()
    private val its = MutableList(mergerEnvironment.dataHoldersCount) { 0 }

    init {
        for (holderId in 0 until mergerEnvironment.dataHoldersCount) {
            if (prevStepBatches != null && prevStepBatches.containsKey(holderId)) {
                batches[holderId] = prevStepBatches[holderId]!!
            } else {
                val newBatch = mergerEnvironment.requestBatch(holderId)
                if (newBatch.isNotEmpty()) {
                    batches[holderId] = newBatch
                }
            }
        }
        for (entry in batches.entries) {
            h.add(Pair(entry.value.first(), entry.key))
            its[entry.key]++
        }
    }

    private fun getValueFromBatch(holderId: Int): T? {
        val values = batches[holderId] ?: return null
        if (values.size == its[holderId]) {
            // we have to request a new batch here
            val newValues = mergerEnvironment.requestBatch(holderId)
            its[holderId] = 0
            batches[holderId] = newValues
            if (newValues.isEmpty()) {
                return null
            }
            its[holderId]++
            return newValues[0]
        }
        return values[its[holderId]++]
    }

    override fun mergeStep(): T? {
        val p = h.pollFirst()
        return if (p != null) {
            val res = p.first
            val holderId = p.second
            val value = getValueFromBatch(holderId)
            if (value != null) {
                h.add(Pair(value, holderId))
            }
            res
        } else {
            null
        }
    }

    override fun getRemainingBatches(): Map<Int, List<T>> {
        val res = mutableMapOf<Int, List<T>>()
        for (entry in batches.entries) {
            val holderId = entry.key
            val list = entry.value
            val fromHeap = h.find { it.second == holderId }
            if (list.size == its[holderId]) {
                if (fromHeap != null) {
                    res[holderId] = listOf(fromHeap.first)
                }
                continue
            }
            if (fromHeap == null) {
                res[holderId] = list.subList(its[holderId], list.size)
            } else {
                res[holderId] = listOf(fromHeap.first) + list.subList(its[holderId], list.size)
            }
        }
        return Collections.unmodifiableMap(res)
    }
}