import kotlin.collections.HashMap

class ConsistentHashImpl<K> : ConsistentHash<K> {
    private var cntShards = 0
    private val shardToHash = HashMap<Shard, MutableList<Int>>()
    private val shardHashes = sortedSetOf<Int>() //to find hash of needed shard
    private val hashToShard = HashMap<Int, Shard>()

    private fun addToShardToHash(shard: Shard, hashes: Set<Int>) {
        if (!shardToHash.containsKey(shard)) {
            shardToHash[shard] = mutableListOf()
        }
        shardToHash[shard]?.addAll(hashes)
    }

    private fun findNextOrEqHash(hash: Int): Int {
        return shardHashes.ceiling(hash) ?: shardHashes.first()
    }

    private fun findPrevOrEqHash(hash: Int): Int {
        return shardHashes.floor(hash) ?: shardHashes.last()
    }

    private fun findNextHash(hash: Int): Int {
        return shardHashes.higher(hash) ?: shardHashes.first()
    }

    private fun findPrevHash(hash: Int): Int {
        return shardHashes.lower(hash) ?: shardHashes.last()
    }

    private fun normalHashRangeIn(a: HashRange, b: HashRange): Boolean {
        return a.leftBorder >= b.leftBorder && a.rightBorder <= b.rightBorder
    }

    private fun hashRangeIn(a: HashRange, b: HashRange): Boolean { // check is a in b
        val a1 = HashRange(a.leftBorder, Int.MAX_VALUE)
        val a2 = HashRange(Int.MIN_VALUE, a.rightBorder)
        val b1 = HashRange(b.leftBorder, Int.MAX_VALUE)
        val b2 = HashRange(Int.MIN_VALUE, b.rightBorder)
        if (a.leftBorder <= a.rightBorder && b.leftBorder <= b.rightBorder) {
            return normalHashRangeIn(a, b)
        }
        if (a.leftBorder > a.rightBorder && b.leftBorder <= b.rightBorder) {
            return false
        }
        if (a.leftBorder <= a.rightBorder && b.leftBorder > b.rightBorder) {
            return normalHashRangeIn(a, b1) || normalHashRangeIn(a, b2)
        }
        return normalHashRangeIn(a1, b1) && normalHashRangeIn(a2, b2)
    }

    private fun leftConnect(a: HashRange, b: HashRange): Boolean {
        return a.rightBorder + 1 == b.leftBorder
    }

    override fun getShardByKey(key: K): Shard {
        val hash = key.hashCode()
        return hashToShard[findNextOrEqHash(hash)]!!
    }

    override fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>> {
        val res = mutableMapOf<Shard, MutableSet<HashRange>>()
        if (cntShards > 0) {
            for (hash in vnodeHashes) {
                val nextHash = findNextOrEqHash(hash)
                val prevHash = findPrevOrEqHash(hash) // out hash range is [prevHash + 1; hash]
                val curHashRange = HashRange(prevHash + 1, hash)
                val nextShard = hashToShard[nextHash]!!
                if (!res.containsKey(nextShard)) {
                    res[nextShard] = mutableSetOf()
                }
                var toAdd = true
                for (hr in res[nextShard]!!) {
                    if (hashRangeIn(curHashRange, hr)) {
                        toAdd = false
                        break
                    }
                }
                if (toAdd) {
                    val curRes = HashSet<HashRange>()
                    curRes.addAll(res[nextShard]!!)
                    for (hr in curRes) {
                        if (hashRangeIn(hr, curHashRange)) {
                            res[nextShard]!!.remove(hr)
                        }
                    }
                    res[nextShard]!!.add(curHashRange)
                }
            }
        }
        addToShardToHash(newShard, vnodeHashes)
        shardHashes.addAll(vnodeHashes)
        for (hash in vnodeHashes) {
            hashToShard[hash] = newShard
        }
        cntShards++
        return res
    }

    override fun removeShard(shard: Shard): Map<Shard, Set<HashRange>> {
        val res = mutableMapOf<Shard, MutableSet<HashRange>>()
        val hashes = shardToHash[shard]!!
        for (hash in hashes) {
            val nextHash = findNextHash(hash)
            val prevHash = findPrevHash(hash)
            var curHashRange = HashRange(prevHash + 1, hash)
            val nextShard = hashToShard[nextHash]!!
            if (!res.containsKey(nextShard)) {
                res[nextShard] = mutableSetOf()
            }
            val curRes = HashSet<HashRange>()
            curRes.addAll(res[nextShard]!!)
            for (has in curRes) {
                if (leftConnect(curHashRange, has)) {
                    curHashRange = HashRange(curHashRange.leftBorder, has.rightBorder)
                    res[nextShard]!!.remove(has)
                } else {
                    if (leftConnect(has, curHashRange)) {
                        curHashRange = HashRange(has.leftBorder, curHashRange.rightBorder)
                        res[nextShard]!!.remove(has)
                    }
                }
            }
            res[nextShard]!!.add(curHashRange)
            shardHashes.remove(hash)
            hashToShard.remove(hash)
        }
        res.remove(shard)
        shardToHash.remove(shard)
        cntShards--
        return res
    }
}