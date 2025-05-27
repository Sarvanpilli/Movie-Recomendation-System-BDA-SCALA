import com.mongodb.client._
import com.mongodb.client.model._
import com.mongodb.client.model.Filters._
import com.mongodb.client.model.Updates._
import com.mongodb.client.model.Sorts._
import com.mongodb.client.model.Indexes._
import com.mongodb.client.model.IndexOptions
import org.bson.Document
import org.bson.conversions.Bson
import java.time.LocalDateTime
import scala.jdk.CollectionConverters._

object MongoDBConnection {
  private val mongoClient: MongoClient = MongoClients.create("mongodb://localhost:27017")
  private val database: MongoDatabase = mongoClient.getDatabase("moviemate")
  private val quizCollection: MongoCollection[Document] = database.getCollection("quiz_responses")

  // Initialize indexes
  def initializeIndexes(): Unit = {
    val indexOptions = new IndexOptions().unique(true)
    val keys = Indexes.ascending("question", "answered_at")
    quizCollection.createIndex(keys, indexOptions)
  }

  // Insert a quiz response
  def insertQuizResponse(
    question: String,
    questionType: String,
    userAnswer: String,
    correctAnswer: String,
    isCorrect: Boolean,
    answeredAt: LocalDateTime
  ): Unit = {
    val doc = new Document()
      .append("question", question)
      .append("type", questionType)
      .append("user_answer", userAnswer)
      .append("correct_answer", correctAnswer)
      .append("is_correct", isCorrect)
      .append("answered_at", answeredAt.toString)
    
    quizCollection.insertOne(doc)
  }

  // Get all quiz responses
  def getAllQuizResponses(): List[Document] = {
    quizCollection.find().into(new java.util.ArrayList[Document]()).asScala.toList
  }

  // Get quiz responses by type
  def getQuizResponsesByType(questionType: String): List[Document] = {
    val filter = Filters.eq("type", questionType)
    quizCollection
      .find(filter)
      .into(new java.util.ArrayList[Document]())
      .asScala
      .toList
  }

  // Get quiz responses by date range
  def getQuizResponsesByDateRange(startDate: LocalDateTime, endDate: LocalDateTime): List[Document] = {
    val filter = Filters.and(
      Filters.gte("answered_at", startDate.toString),
      Filters.lte("answered_at", endDate.toString)
    )
    quizCollection
      .find(filter)
      .into(new java.util.ArrayList[Document]())
      .asScala
      .toList
  }

  // Close the MongoDB connection
  def close(): Unit = {
    mongoClient.close()
  }
} 