import scala.io.Source
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Using
import MongoDBConnection._

object MigrateToMongoDB {
  def main(args: Array[String]): Unit = {
    println("Starting migration from CSV to MongoDB...")
    
    // Initialize MongoDB indexes
    initializeIndexes()
    
    // Read and migrate data from CSV
    val csvFile = "quiz_responses.csv"
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    
    Using(Source.fromFile(csvFile)) { source =>
      val lines = source.getLines().drop(1) // Skip header
      
      lines.foreach { line =>
        val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
        
        // Remove quotes and unescape doubled quotes
        val question = fields(0).replaceAll("^\"|\"$", "").replace("\"\"", "\"")
        val questionType = fields(1)
        val userAnswer = fields(2).replaceAll("^\"|\"$", "").replace("\"\"", "\"")
        val correctAnswer = fields(3).replaceAll("^\"|\"$", "").replace("\"\"", "\"")
        val isCorrect = fields(4).toBoolean
        val timestamp = LocalDateTime.parse(fields(5), formatter)
        
        // Insert into MongoDB
        insertQuizResponse(
          question = question,
          questionType = questionType,
          userAnswer = userAnswer,
          correctAnswer = correctAnswer,
          isCorrect = isCorrect,
          answeredAt = timestamp
        )
      }
    }
    
    println("Migration completed successfully!")
    
    // Verify the migration
    val totalResponses = getAllQuizResponses().size
    println(s"Total responses in MongoDB: $totalResponses")
    
    // Close MongoDB connection
    close()
  }
} 