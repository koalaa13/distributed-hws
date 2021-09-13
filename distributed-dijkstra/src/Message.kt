package dijkstra.messages

sealed class Message

data class MessageWithDistance(val data: Long) : Message()

object AckMessage : Message()

object AddChildMessage : Message()

object DeleteChildMessage : Message()