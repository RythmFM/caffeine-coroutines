package fm.rythm.caffeine

import com.github.benmanes.caffeine.cache.AsyncCache
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.asExecutor
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.future.future
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Executor

interface DeferredCoroutineCache<K : Any, V> {

    fun getIfPresentAsync(key: K): Deferred<V>?

    fun getAsync(key: K, mappingFunction: suspend (K) -> V): Deferred<V>

    fun getAllAsync(keys: Collection<K>, mappingFunction: suspend (Collection<K>) -> Map<K, V>): Deferred<Map<K, V>>

    fun put(key: K, value: V)

    fun asMap(): ConcurrentMap<K, CompletableFuture<V>>

    fun syncCache(): Cache<K, V>

    fun asyncCache(): AsyncCache<K, V>

    fun suspendingCache(): SuspendingCoroutineCache<K, V>
}

interface SuspendingCoroutineCache<K : Any, V> {

    suspend fun getIfPresent(key: K): V?

    suspend fun get(key: K, mappingFunction: suspend (K) -> V): V

    suspend fun getAll(keys: Collection<K>, mappingFunction: suspend (Collection<K>) -> Map<K, V>): Map<K, V>

    fun put(key: K, value: V)

    fun asMap(): ConcurrentMap<K, CompletableFuture<V>>

    fun syncCache(): Cache<K, V>

    fun asyncCache(): AsyncCache<K, V>

    fun deferredCache(): DeferredCoroutineCache<K, V>
}

class DeferredCoroutineCacheImpl<K : Any, V>(
    private val asyncCache: AsyncCache<K, V>
) : DeferredCoroutineCache<K, V> {
    override fun getIfPresentAsync(key: K): Deferred<V>? =
        asyncCache.getIfPresent(key)
            ?.asDeferred()

    override fun getAsync(key: K, mappingFunction: suspend (K) -> V): Deferred<V> =
        asyncCache.get(key) { neededKey, executor ->
            executor.future { mappingFunction(neededKey) }
        }.asDeferred()

    override fun getAllAsync(
        keys: Collection<K>,
        mappingFunction: suspend (Collection<K>) -> Map<K, V>
    ): Deferred<Map<K, V>> =
        asyncCache.getAll(keys) { neededKeys, executor ->
            executor.future {
                @Suppress("UNCHECKED_CAST")
                mappingFunction(neededKeys as Collection<K>)
            }
        }.asDeferred()

    override fun put(key: K, value: V) {
        asyncCache.put(key, CompletableFuture.completedFuture(value))
    }

    override fun asMap(): ConcurrentMap<K, CompletableFuture<V>> =
        asyncCache.asMap()

    override fun asyncCache(): AsyncCache<K, V> =
        asyncCache

    override fun suspendingCache(): SuspendingCoroutineCache<K, V> =
        SuspendingCoroutineCacheImpl(this)

    override fun syncCache(): Cache<K, V> =
        asyncCache.synchronous()

    private fun <T> Executor.future(block: suspend CoroutineScope.() -> T) =
        CoroutineScope(asCoroutineDispatcher())
            .future(block = block)
}

class SuspendingCoroutineCacheImpl<K : Any, V>(
    private val deferredCache: DeferredCoroutineCache<K, V>
) : SuspendingCoroutineCache<K, V> {
    override suspend fun getIfPresent(key: K): V? =
        deferredCache.getIfPresentAsync(key)
            ?.await()

    override suspend fun get(key: K, mappingFunction: suspend (K) -> V): V =
        deferredCache.getAsync(key, mappingFunction)
            .await()

    override suspend fun getAll(
        keys: Collection<K>,
        mappingFunction: suspend (Collection<K>) -> Map<K, V>
    ): Map<K, V> =
        deferredCache.getAllAsync(keys, mappingFunction)
            .await()

    override fun put(key: K, value: V) {
        deferredCache.put(key, value)
    }

    override fun asMap(): ConcurrentMap<K, CompletableFuture<V>> =
        deferredCache.asMap()

    override fun deferredCache(): DeferredCoroutineCache<K, V> =
        deferredCache

    override fun asyncCache(): AsyncCache<K, V> =
        deferredCache.asyncCache()

    override fun syncCache(): Cache<K, V> =
        deferredCache.syncCache()
}

fun <K : Any, V, K1 : K, V1 : V> Caffeine<K, V>.buildDeferred(): DeferredCoroutineCache<K1, V1> =
    DeferredCoroutineCacheImpl(asyncCache = buildAsync())

fun <K : Any, V, K1 : K, V1 : V> Caffeine<K, V>.buildSuspending(): SuspendingCoroutineCache<K1, V1> =
    buildDeferred<K, V, K1, V1>()
        .suspendingCache()

fun <K, V> Caffeine<K, V>.dispatcher(dispatcher: CoroutineDispatcher) =
    executor(dispatcher.asExecutor())