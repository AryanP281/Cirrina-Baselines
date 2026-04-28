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
      register(StatisticsRequest::class.java)
      register(MessageProcessingRequest::class.java)
      register(PhotoScanResponse::class.java)
      register(PickupResponse::class.java)
      register(AssembleResponse::class.java)
    }

  private val threadBuffer = ThreadLocal.withInitial { MemoryBuffer.newHeapBuffer(1024) }
  private val client = HttpClient.newHttpClient()
  var baseUrl = "http://localhost:6000"

  fun processEmail(req : MessageProcessingRequest) : Mono<Void>
  {
    val buffer = threadBuffer.get().apply { writerIndex(0) }
    fory.serialize(buffer, req)

    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/process/email"))
      .header("Content-Type", "application/x-fury")
      .POST(HttpRequest.BodyPublishers.ofByteArray(buffer.getBytes(0, buffer.writerIndex())))
      .build()

    client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())

    return Mono.empty()
  }

  fun beamDetectionStart(): Mono<BeamDetectionResponse> {
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/detectbeam/start"))
      .GET()
      .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
        .thenApply { response ->
          fory.deserialize(response.body()) as BeamDetectionResponse
        }
    )
  }

  fun beamDetectionEnd(): Mono<BeamDetectionResponse> {
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/detectbeam/end"))
      .GET()
      .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
        .thenApply { response -> fory.deserialize(response.body()) as BeamDetectionResponse}
    )
  }

  fun sendStatistics(req : StatisticsRequest) : Mono<Void> {
    val buffer = threadBuffer.get().apply { writerIndex(0) }
    fory.serialize(buffer, req)

    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/statistics"))
      .header("Content-Type", "application/x-fury")
      .POST(HttpRequest.BodyPublishers.ofByteArray(buffer.getBytes(0, buffer.writerIndex())))
      .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
        null
      }
    )
  }

  fun takePhoto() : Mono<Void>
  {
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/takePhoto"))
      .GET()
      .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
        null
      }
    )

  }

  fun scanPhoto() : Mono<PhotoScanResponse>
  {
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/scanphoto"))
      .GET()
      .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).thenApply { response ->
        fory.deserialize(response.body()) as PhotoScanResponse
      }
    )
  }

  fun moveBelt() : Mono<Void>
  {
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/movebelt"))
      .GET()
      .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
        null
      }
    )
  }

  fun stopBelt() : Mono<Void>
  {
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/stopbelt"))
      .GET()
      .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
        null
      }
    )
  }

  fun pickUp() : Mono<PickupResponse>
  {
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/pickup"))
      .GET()
      .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).thenApply { response ->
        fory.deserialize(response.body()) as PickupResponse
      }
    )
  }

  fun assemble() : Mono<AssembleResponse>
  {
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/assemble"))
      .GET()
      .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).thenApply { response ->
        fory.deserialize(response.body()) as AssembleResponse
      }
    )
  }

  fun returnToStart() : Mono<Void>
  {
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/returntostart"))
      .GET()
      .build()

    return Mono.fromFuture(
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
        null
      }
    )
  }

}
