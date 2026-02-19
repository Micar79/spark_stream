from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, explode, array_intersect, size, collect_set
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import functions as F
import re

# ────────────────────────────────────────────────
# Your extracted CV skills (normalized to lowercase)
# ────────────────────────────────────────────────
user_skills = [
    'python', 'sql', 'pyspark', 'r', 'aws', 's3', 'rds', 'ec2', 'vpc', 'lambda', 'sns',
    'kinesis', 'cloudformation', 'etl', 'elt', 'streaming', 'data quality', 'data profiling',
    'event-driven', 'ml deployment', 'yolov7', 'rekognition', 'streamlit', 'power bi',
    'looker studio', 'generative ai', 'llm', 'llms', 'openai', 'rag', 'prompt engineering',
    'vector database', 'qdrant', 'pinecone', 'embeddings', 'semantic search', 'hallucination reduction'
]

# ────────────────────────────────────────────────
# Simple regex-based skill extractor from job description
# ────────────────────────────────────────────────
def extract_skills(text):
    if not text:
        return []
    text = text.lower()
    found = set()
    for skill in user_skills:
        # Look for whole word or common variations
        if re.search(r'\b' + re.escape(skill) + r'\b', text) or \
           re.search(r'\b' + re.escape(skill.replace(' ', '')) + r'\b', text):
            found.add(skill)
    return list(found)

extract_udf = F.udf(extract_skills, ArrayType(StringType()))

# ────────────────────────────────────────────────
# Main pipeline
# ────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Personal Job Matcher - Michael Nthome") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("Spark session started.")

try:
    # Read job postings CSV
    df = spark.read.option("header", "true").csv("DataEngineer.csv", multiLine=True, escape='"')

    print(f"Loaded {df.count()} job postings.")

    # Clean and extract skills
    df_clean = df.select(
        F.col("Job Title").alias("title"),
        F.col("Company").alias("company"),
        F.col("Location").alias("location"),
        F.col("Salary Estimate").alias("salary"),
        F.col("Job Description").alias("description")
    ).na.drop(subset=["description"])

    df_with_skills = df_clean.withColumn(
        "job_skills",
        extract_udf(F.col("description"))
    ).filter(F.size("job_skills") > 0)

    # Compute matches & score
    df_scored = df_with_skills.withColumn(
        "matching_skills",
        array_intersect(F.col("job_skills"), F.lit(user_skills))
    ).withColumn(
        "match_score",
        size("matching_skills")
    ).withColumn(
        "match_percentage",
        (F.size("matching_skills") / F.size(F.lit(user_skills)) * 100).cast("decimal(5,2)")
    )

    # Top matches
    top_matches = df_scored.orderBy(F.col("match_score").desc(), F.col("match_percentage").desc()) \
                           .limit(20)

    print("\nTop 20 matching jobs:")
    top_matches.select(
        "title", "company", "location", "salary",
        "match_score", "match_percentage", "matching_skills"
    ).show(20, truncate=80)

    # Save results
    top_matches.write.mode("overwrite").option("header", "true").csv("matched_jobs_output")

    print("\nResults saved to folder: matched_jobs_output/")

except Exception as e:
    print("Error occurred:")
    print(e)
    print("\nPossible fixes:")
    print("1. Make sure 'data_jobs.csv' exists in the current directory")
    print("2. Check file encoding / quotes (multiLine=True should help)")
    print("3. Try a different dataset from Kaggle if needed")

finally:
    spark.stop()
    print("Spark session stopped.")