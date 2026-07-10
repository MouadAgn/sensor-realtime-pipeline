"""
Complete setup script for the sensor pipeline on Databricks.

This script will:
1. Create required Unity Catalog volumes
2. Upload CSV reference files
3. Create test Bronze data (since Kafka is not configured)
4. Verify the setup is complete

Run this FIRST before running any other pipeline scripts.
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("pipeline-setup").getOrCreate()

print("=" * 80)
print("SENSOR PIPELINE SETUP - DATABRICKS")
print("=" * 80)

# Get current catalog and schema
current_catalog = spark.catalog.currentCatalog()
current_schema = spark.catalog.currentDatabase()
print(f"\n>>> Using catalog: {current_catalog}, schema: {current_schema}")

# ============================================================================
# STEP 1: Create Unity Catalog Volumes
# ============================================================================
print("\n" + "=" * 80)
print("STEP 1: Creating Unity Catalog Volumes")
print("=" * 80)

volumes_to_create = [
    ("checkpoints", "For streaming checkpoint data"),
    ("data_ref", "For reference CSV files")
]

for volume_name, description in volumes_to_create:
    try:
        spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {current_catalog}.{current_schema}.{volume_name}
            COMMENT '{description}'
        """)
        print(f"✅ Volume '{volume_name}' ready")
    except Exception as e:
        print(f"❌ Error creating volume '{volume_name}': {e}")

# ============================================================================
# STEP 2: Check CSV Files
# ============================================================================
print("\n" + "=" * 80)
print("STEP 2: Checking CSV Reference Files")
print("=" * 80)

volume_path = f"/Volumes/{current_catalog}/{current_schema}/data_ref"
csv_files = ["capteurs.csv", "machines.csv", "sites.csv", "seuils_machine.csv"]

print(f"\n>>> Expected location: {volume_path}/")

csv_status = {}
for csv_file in csv_files:
    csv_path = f"{volume_path}/{csv_file}"
    try:
        # Try to read the file to check if it exists
        test_df = spark.read.option("header", "true").csv(csv_path).limit(1)
        csv_status[csv_file] = True
        print(f"✅ Found: {csv_file}")
    except Exception:
        csv_status[csv_file] = False
        print(f"❌ Missing: {csv_file}")

# ============================================================================
# STEP 3: Upload CSV Files (if missing)
# ============================================================================
if not all(csv_status.values()):
    print("\n" + "=" * 80)
    print("STEP 3: CSV Files Need Upload")
    print("=" * 80)
    print(f"\n⚠️  Some CSV files are missing!")
    print(f"\n📋 To upload the CSV files:")
    print(f"   1. Use dbutils.fs.cp to copy from workspace to volume:")
    print(f"\n      # Copy CSV files to volume")
    source_path = "/Workspace/Users/faycal.arkance@gmail.com/sensor-realtime-pipeline/data"
    for csv_file, exists in csv_status.items():
        if not exists:
            print(f"      dbutils.fs.cp('{source_path}/{csv_file}', '{volume_path}/{csv_file}')")
    print(f"\n   OR upload manually via Catalog Explorer:")
    print(f"   - Navigate to: {current_catalog} > {current_schema} > data_ref")
    print(f"   - Click 'Upload Files' and select the CSV files")
else:
    print("\n✅ All CSV files are present!")

# ============================================================================
# STEP 4: Create Test Bronze Data (No Kafka Required)
# ============================================================================
print("\n" + "=" * 80)
print("STEP 4: Creating Test Bronze Data (Kafka Simulation)")
print("=" * 80)

bronze_table = "bronze_sensor_data"

# Check if bronze table already exists
if spark.catalog.tableExists(bronze_table):
    row_count = spark.table(bronze_table).count()
    print(f"\n>>> Table '{bronze_table}' already exists with {row_count} rows")
    print(f"    Overwriting with fresh test data...")
    skip_bronze = False
else:
    print(f"\n>>> Creating new table '{bronze_table}'...")
    skip_bronze = False

if not skip_bronze:
    # Generate comprehensive test data
    test_events = [
        # Temperature sensors
        ('evt1', '{"event_id": "evt1", "capteur_id": "CAPT001", "machine_id": "M001", "site_id": "S001", "type_mesure": "temperature", "valeur": 25.5, "unite": "C", "qualite_signal": 95.0, "batterie_pourcentage": 85, "timestamp": "2024-01-15T10:00:00"}', 0, 0, "2024-01-15T10:00:00", "2024-01-15T10:00:01", "2024-01-15"),
        ('evt2', '{"event_id": "evt2", "capteur_id": "CAPT001", "machine_id": "M001", "site_id": "S001", "type_mesure": "temperature", "valeur": 26.8, "unite": "C", "qualite_signal": 96.0, "batterie_pourcentage": 85, "timestamp": "2024-01-15T10:05:00"}', 0, 1, "2024-01-15T10:05:00", "2024-01-15T10:05:01", "2024-01-15"),
        ('evt3', '{"event_id": "evt3", "capteur_id": "CAPT001", "machine_id": "M001", "site_id": "S001", "type_mesure": "temperature", "valeur": 24.2, "unite": "C", "qualite_signal": 94.0, "batterie_pourcentage": 84, "timestamp": "2024-01-15T10:10:00"}', 0, 2, "2024-01-15T10:10:00", "2024-01-15T10:10:01", "2024-01-15"),
        
        # Pressure sensors
        ('evt4', '{"event_id": "evt4", "capteur_id": "CAPT002", "machine_id": "M001", "site_id": "S001", "type_mesure": "pression", "valeur": 1013.0, "unite": "hPa", "qualite_signal": 98.0, "batterie_pourcentage": 90, "timestamp": "2024-01-15T10:01:00"}', 0, 3, "2024-01-15T10:01:00", "2024-01-15T10:01:01", "2024-01-15"),
        ('evt5', '{"event_id": "evt5", "capteur_id": "CAPT002", "machine_id": "M001", "site_id": "S001", "type_mesure": "pression", "valeur": 1014.5, "unite": "hPa", "qualite_signal": 97.0, "batterie_pourcentage": 90, "timestamp": "2024-01-15T10:06:00"}', 0, 4, "2024-01-15T10:06:00", "2024-01-15T10:06:01", "2024-01-15"),
        
        # Vibration sensors
        ('evt6', '{"event_id": "evt6", "capteur_id": "CAPT003", "machine_id": "M002", "site_id": "S001", "type_mesure": "vibration", "valeur": 0.8, "unite": "mm/s", "qualite_signal": 92.0, "batterie_pourcentage": 78, "timestamp": "2024-01-15T10:02:00"}', 0, 5, "2024-01-15T10:02:00", "2024-01-15T10:02:01", "2024-01-15"),
        ('evt7', '{"event_id": "evt7", "capteur_id": "CAPT003", "machine_id": "M002", "site_id": "S001", "type_mesure": "vibration", "valeur": 1.2, "unite": "mm/s", "qualite_signal": 93.0, "batterie_pourcentage": 78, "timestamp": "2024-01-15T10:07:00"}', 0, 6, "2024-01-15T10:07:00", "2024-01-15T10:07:01", "2024-01-15"),
        
        # Anomalous data (high value)
        ('evt8', '{"event_id": "evt8", "capteur_id": "CAPT001", "machine_id": "M001", "site_id": "S001", "type_mesure": "temperature", "valeur": 85.5, "unite": "C", "qualite_signal": 89.0, "batterie_pourcentage": 84, "timestamp": "2024-01-15T10:15:00"}', 0, 7, "2024-01-15T10:15:00", "2024-01-15T10:15:01", "2024-01-15"),
        
        # Low battery
        ('evt9', '{"event_id": "evt9", "capteur_id": "CAPT004", "machine_id": "M003", "site_id": "S002", "type_mesure": "humidity", "valeur": 65.0, "unite": "%", "qualite_signal": 88.0, "batterie_pourcentage": 15, "timestamp": "2024-01-15T10:03:00"}', 0, 8, "2024-01-15T10:03:00", "2024-01-15T10:03:01", "2024-01-15"),
        
        # More data for aggregations
        ('evt10', '{"event_id": "evt10", "capteur_id": "CAPT005", "machine_id": "M003", "site_id": "S002", "type_mesure": "temperature", "valeur": 22.5, "unite": "C", "qualite_signal": 96.0, "batterie_pourcentage": 82, "timestamp": "2024-01-15T10:08:00"}', 0, 9, "2024-01-15T10:08:00", "2024-01-15T10:08:01", "2024-01-15"),
    ]
    
    columns = ["kafka_key", "payload_json", "kafka_partition", "kafka_offset", "kafka_timestamp", "ts_ingestion", "date_ingestion"]
    
    bronze_df = spark.createDataFrame(test_events, columns)
    bronze_df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    
    final_count = spark.table(bronze_table).count()
    print(f"✅ Created '{bronze_table}' with {final_count} test events")
    print(f"   These events simulate Kafka stream data")

# ============================================================================
# STEP 5: Verify Setup
# ============================================================================
print("\n" + "=" * 80)
print("STEP 5: Setup Verification")
print("=" * 80)

all_good = True

# Check volumes
print("\n📦 Volumes:")
for volume_name, _ in volumes_to_create:
    try:
        spark.sql(f"DESCRIBE VOLUME {current_catalog}.{current_schema}.{volume_name}")
        print(f"   ✅ {volume_name}")
    except:
        print(f"   ❌ {volume_name}")
        all_good = False

# Check CSV files
print("\n📄 CSV Files:")
for csv_file, exists in csv_status.items():
    if exists:
        print(f"   ✅ {csv_file}")
    else:
        print(f"   ⚠️  {csv_file} (needs upload - pipeline can run but Silver/Gold will need this)")

# Check Bronze table
print("\n🗄️  Bronze Table:")
if spark.catalog.tableExists(bronze_table):
    count = spark.table(bronze_table).count()
    print(f"   ✅ {bronze_table} ({count} rows)")
else:
    print(f"   ❌ {bronze_table}")
    all_good = False

# ============================================================================
# FINAL STATUS
# ============================================================================
print("\n" + "=" * 80)
if all_good or (spark.catalog.tableExists(bronze_table) and all([v[0] == "checkpoints" for v in volumes_to_create[:1]])):
    print("✅ SETUP COMPLETE - PIPELINE READY!")
    print("=" * 80)
    print("\n📋 Next Steps:")
    if not all(csv_status.values()):
        print("   0. [OPTIONAL] Upload CSV files for Silver/Gold layers")
    print("   1. Run Silver layer: %run ./spark/apps/silver_clean.py")
    print("   2. Run Gold layer: %run ./spark/apps/gold_pipeline.py")
    print("\n💡 Note: Bronze ingestion is using TEST DATA (Kafka not required)")
else:
    print("⚠️  SETUP INCOMPLETE - ACTION REQUIRED")
    print("=" * 80)
    print("\n📋 Required Actions:")
    if not all(csv_status.values()):
        print("   1. Upload missing CSV files to the data_ref volume (see instructions above)")
    if not spark.catalog.tableExists(bronze_table):
        print("   2. Re-run this setup script to create Bronze test data")
    print("\n💡 Once complete, re-run this setup script to verify")

print("\n" + "=" * 80)
