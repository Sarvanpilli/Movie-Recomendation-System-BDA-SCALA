import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.{Try, Using}
import MongoDBConnection._

object QuizLogger {
  def logResponse(
    question: TriviaQuestion,
    userAnswer: String,
  ): Unit = {
    val correctAnswer = question.answer
    val questionType = question.langType
    val questionText = question.question
    val isCorrect = compareAnswer(question, userAnswer)
    val timestamp = LocalDateTime.now()
    
    // Log to MongoDB
    insertQuizResponse(
      question = questionText,
      questionType = questionType,
      userAnswer = userAnswer,
      correctAnswer = correctAnswer,
      isCorrect = isCorrect,
      answeredAt = timestamp
    )
  }

  def compareAnswer(question: TriviaQuestion, userAnswer: String): Boolean = {
    val similarityThreshold = 0.25 // Adjust this threshold as needed
    
    // Find the index of the correct answer in the choices list
    val correctAnswerIndex = question.choices.indexWhere(_.toLowerCase.trim == question.answer.toLowerCase.trim) + 1
    
    // Try to match against the correct answer
    val answerSimilarity = answerSimilarityScore(userAnswer, question.answer, correctAnswerIndex)
    
    (answerSimilarity >= similarityThreshold)     
  }

  private def answerSimilarityScore(userAnswer: String, correctAnswer: String, correctIndex: Int): Double = {
    val normalizedUser = userAnswer.trim.toLowerCase
    val normalizedCorrect = correctAnswer.trim.toLowerCase
    val tokens = normalizedUser.split("\\s+").toSet

    val numericEquivalents = Map(
      "1" -> Set("first", "one", "1st", "1", "a"),
      "2" -> Set("second", "two", "2nd", "2", "b"),
      "3" -> Set("third", "three", "3rd", "3", "c"),
      "4" -> Set("fourth", "four", "4th", "4", "d")
    )

    val isDirectMatch = normalizedUser == normalizedCorrect

    // Get the numeric equivalents for the correct index
    val correctIndexStr = correctIndex.toString
    val validAnswers = numericEquivalents.getOrElse(correctIndexStr, Set.empty)

    // Check if user answer matches any valid form of the correct index
    val isNumericEquivalent = tokens.exists(word => validAnswers.contains(word))

    if (isDirectMatch || isNumericEquivalent) 1.0
    else {
      val userWords = normalizedUser.split("\\s+").toSet
      val correctWords = normalizedCorrect.split("\\s+").toSet
      val commonWords = userWords.intersect(correctWords)
      val totalWords = userWords.union(correctWords)

      val wordOverlapScore =
        if (totalWords.nonEmpty) commonWords.size.toDouble / totalWords.size
        else 0.0

      val maxLength = math.max(normalizedUser.length, normalizedCorrect.length)
      val levenshteinScore =
        if (maxLength > 0)
          1.0 - (levenshteinDistance(normalizedUser, normalizedCorrect).toDouble / maxLength)
        else 0.0

      if (levenshteinScore >= 0.75) 1.0 // typo forgiveness
      else {
        val weightedScore = (wordOverlapScore * 0.6) + (levenshteinScore * 0.4)
        math.min(1.0, weightedScore)
      }
    }
  }

  // Helper function to calculate Levenshtein distance
  private def levenshteinDistance(s1: String, s2: String): Int = {
    val m = s1.length
    val n = s2.length
    val dp = Array.ofDim[Int](m + 1, n + 1)
    
    for (i <- 0 to m) dp(i)(0) = i
    for (j <- 0 to n) dp(0)(j) = j
    
    for (i <- 1 to m; j <- 1 to n) {
      dp(i)(j) = if (s1(i - 1) == s2(j - 1)) {
        dp(i - 1)(j - 1)
      } else {
        math.min(
          math.min(dp(i - 1)(j) + 1, dp(i)(j - 1) + 1),
          dp(i - 1)(j - 1) + 1
        )
      }
    }
    
    dp(m)(n)
  }

  def getQuizHistory(): List[QuizResponse] = {
    val responses = getAllQuizResponses()
    responses.map { doc =>
      QuizResponse(
        question = doc.getString("question"),
        questionType = doc.getString("type"),
        userAnswer = doc.getString("user_answer"),
        correctAnswer = doc.getString("correct_answer"),
        isCorrect = doc.getBoolean("is_correct"),
        answeredAt = LocalDateTime.parse(doc.getString("answered_at"))
      )
    }
  }
}

case class QuizResponse(
  question: String,
  questionType: String,
  userAnswer: String,
  correctAnswer: String,
  isCorrect: Boolean,
  answeredAt: LocalDateTime
) 