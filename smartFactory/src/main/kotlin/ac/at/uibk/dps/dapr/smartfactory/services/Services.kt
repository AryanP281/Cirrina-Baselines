package ac.at.uibk.dps.dapr.smartfactory.services

import org.apache.fory.Fory
import org.apache.fory.ThreadSafeFory
import org.apache.fory.config.Language
import org.apache.fory.memory.MemoryBuffer
import reactor.core.publisher.Mono
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

object Services {

  private val fory: ThreadSafeFory =
    Fory.builder().withLanguage(Language.XLANG).withRefTracking(true).buildThreadSafeFory().apply {
      register(EmptyRequest::class.java)
      register(BeamDetectionResponse::class.java)
    }

  private val threadBuffer = ThreadLocal.withInitial { MemoryBuffer.newHeapBuffer(1024) }
  private val client = HttpClient.newHttpClient()
  private val baseUrl = "http://localhost:6000"

  fun beamDetectionStart(): Mono<BeamDetectionResponse> {
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/detectbeam/start"))
      .GET()
      .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
        .thenApply { response -> fory.deserialize(response.body()) as BeamDetectionResponse}
    )
  }
}
