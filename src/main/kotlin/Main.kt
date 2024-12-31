package org.example

import com.mongodb.client.model.Filters
import com.mongodb.client.model.Updates
import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoCollection
import com.mongodb.kotlin.client.coroutine.MongoDatabase
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.types.ObjectId

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
fun main() {

    val database = getDatabase()
    val collection = database.getCollection<MoviesInfo>(collectionName = "movies")

    runBlocking {

    }

}


fun getDatabase(): MongoDatabase {

    val client = MongoClient.create(connectionString = System.getenv("MONGO_URI"))
    return client.getDatabase(databaseName = "sample_mflix")

}

data class MoviesInfo(
    val plot: String? = "",
    val title: String? = "",
    val poster: String? = "",
    val type: String? = "",
)

suspend fun addMovie(database: MongoDatabase) {

    val info = MoviesInfo(
        plot = "A group of nibba, nibbi who find the true love in a forest",
        title = "Nibba Nibbi True Love Story",
        poster = "https://m.media-amazon.com/images/I/81OSz-XeArL._AC_UF1000,1000_QL80_.jpg",
        type = "movie",
    )

    val collection = database.getCollection<MoviesInfo>(collectionName = "movies")
    collection.insertOne(info).also { println(it.insertedId) }

}

suspend fun readMovie(collection: MongoCollection<MoviesInfo>) {

    val query = Filters.or(
        Filters.eq("type", "movie"),

        )
    val documents = collection.find<MoviesInfo>(filter = query)
    println("Query results: $documents")


    collection.find<MoviesInfo>(filter = query).limit(20).collect {
        println(it)
    }
}

suspend fun updateMovie(collection: MongoCollection<MoviesInfo>) {
    val query = Filters.eq(MoviesInfo::title.name, "Nibba Nibbi True Love Story")
    val updateSet = Updates.set(MoviesInfo::title.name, "Nibba Nibbi Ki Pyar Bhari Dastan")

    collection.updateMany(filter = query, update = updateSet).also {
        println("Matched Documents  ${it.matchedCount}  updates documents ${it.modifiedCount}   ")
    }

}

suspend fun deleteMovie(collection: MongoCollection<MoviesInfo>) {
    val query = Filters.eq(MoviesInfo::title.name, "Nibba Nibbi Ki Pyar Bhari Dastan")
    collection.deleteMany(filter = query).also { println("Matched Documents  ${it.deletedCount}") }


}

suspend fun removeIdMovie(collection: MongoCollection<MoviesInfo>, id: String) {

    val result = collection.updateOne(
        Filters.eq("_id", ObjectId(id)),
        Updates.unset("id")
    )

    println("Matched Documents  ${result.matchedCount}")
}










