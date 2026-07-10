"""
Quick Kafka Configuration for Confluent Cloud

After signing up at https://confluent.cloud, run this to configure your credentials.
"""
import os

print("=" * 80)
print("KAFKA CONFIGURATION - CONFLUENT CLOUD")
print("=" * 80)

# STEP 1: Configure your Confluent Cloud credentials here
KAFKA_BOOTSTRAP = "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"  # Replace with your bootstrap server
KAFKA_API_KEY = "YOUR_API_KEY"                                      # Replace with your API key
KAFKA_API_SECRET = "YOUR_API_SECRET"                                # Replace with your API secret
KAFKA_TOPIC = "sensors-data"                                        # Topic name

print("\n📋 Current Configuration:")
print(f"   Bootstrap Server: {KAFKA_BOOTSTRAP}")
print(f"   API Key: {KAFKA_API_KEY}")
print(f"   Topic: {KAFKA_TOPIC}")

# Set environment variables
os.environ["KAFKA_BOOTSTRAP"] = KAFKA_BOOTSTRAP
os.environ["KAFKA_API_KEY"] = KAFKA_API_KEY
os.environ["KAFKA_API_SECRET"] = KAFKA_API_SECRET
os.environ["KAFKA_TOPIC"] = KAFKA_TOPIC

print("\n✅ Environment variables set!")

# Test Kafka connectivity
print("\n" + "=" * 80)
print("TESTING KAFKA CONNECTION")
print("=" * 80)

try:
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("kafka-test").getOrCreate()
    
    print("\n>>> Attempting to connect to Kafka...")
    
    # Try to read from Kafka (will fail fast if connection is bad)
    test_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", 
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";')
        .load()
    )
    
    print("✅ Kafka stream created successfully!")
    print("✅ Connection looks good!")
    print("\n💡 You can now run bronze_ingest.py")
    
except Exception as e:
    print(f"❌ Connection failed: {str(e)}")
    print("\n⚠️  Check:")
    print("   1. Bootstrap server is correct")
    print("   2. API Key and Secret are correct")
    print("   3. Topic exists in Confluent Cloud")
    print("   4. Your Databricks cluster can reach Confluent Cloud")

print("\n" + "=" * 80)
