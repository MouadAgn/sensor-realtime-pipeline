# 🚀 Start Local Kafka with Docker

Complete guide to run Kafka locally and connect it to Databricks.

---

## ⚠️ IMPORTANT: Connectivity Challenge

**Problem:** Databricks serverless runs in AWS cloud and **cannot reach `localhost`** on your machine.

**Solutions:**
1. ✅ **ngrok** - Expose Kafka publicly (recommended for testing)
2. ✅ **Confluent Cloud** - Managed Kafka (easiest, no setup needed)
3. ❌ **Deploy to AWS** - Complex, requires VPC setup

This guide uses **ngrok** to expose your local Kafka.

---

## 📋 Prerequisites

1. **Docker Desktop** installed and running
2. **ngrok** account (free tier works) - https://ngrok.com/signup

---

## 🔧 Step 1: Start Kafka Infrastructure (2 min)

Open terminal in your project root:

```bash
cd /path/to/sensor-realtime-pipeline

# Start Kafka, Kafka UI, Postgres, Metabase
docker compose up -d

# Wait for services to be healthy (30 seconds)
docker compose ps

# Check Kafka is running
docker compose logs kafka | tail -20
```

**Expected output:**
```
✅ kafka         ... Up (healthy)
✅ kafka-init    ... Exit 0
✅ kafka-ui      ... Up
✅ postgres      ... Up (healthy)
✅ metabase      ... Up
```

**Access Kafka UI:** http://localhost:8080

---

## 🎲 Step 2: Start Data Generator (1 min)

```bash
# Start the sensor data generator
docker compose --profile generator up -d generator

# Check it's sending data
docker compose logs generator -f

# You should see:
# >>> Sending event evt-xxxxx to sensors-data
```

**Verify in Kafka UI:**
1. Go to http://localhost:8080
2. Click "Topics" → "sensors-data"
3. Click "Messages" - you should see sensor events flowing!

---

## 🌐 Step 3: Expose Kafka with ngrok (2 min)

### A. Sign up & Install ngrok

```bash
# Sign up at https://ngrok.com/signup (free tier)

# Install ngrok
# Mac:
brew install ngrok

# Windows:
# Download from https://ngrok.com/download

# Linux:
curl -s https://ngrok-agent.s3.amazonaws.com/ngrok.asc | sudo tee /etc/apt/trusted.gpg.d/ngrok.asc >/dev/null
echo "deb https://ngrok-agent.s3.amazonaws.com buster main" | sudo tee /etc/apt/sources.list.d/ngrok.list
sudo apt update && sudo apt install ngrok

# Authenticate ngrok (get token from dashboard)
ngrok config add-authtoken YOUR_NGROK_TOKEN
```

### B. Expose Kafka Port

```bash
# Expose Kafka port 29092
ngrok tcp 29092
```

**Important:** Keep this terminal open! Note the forwarding address:

```
Forwarding: tcp://0.tcp.ngrok.io:12345 -> localhost:29092
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            This is your public Kafka address!
```

---

## ⚙️ Step 4: Configure Databricks

In Databricks, run this cell (replace with YOUR ngrok address):

```python
import os

# REPLACE with your ngrok forwarding address from Step 3
NGROK_HOST = "0.tcp.ngrok.io"      # Example: 0.tcp.ngrok.io
NGROK_PORT = "12345"                # Example: 12345

# Set Kafka configuration
os.environ["KAFKA_BOOTSTRAP"] = f"{NGROK_HOST}:{NGROK_PORT}"
os.environ["KAFKA_TOPIC"] = "sensors-data"

# No authentication needed for local Kafka
# (ngrok handles the tunnel, Kafka is in plaintext mode)

print("✅ Kafka configured!")
print(f"   Bootstrap: {os.environ['KAFKA_BOOTSTRAP']}")
print(f"   Topic: {os.environ['KAFKA_TOPIC']}")
```

---

## ▶️ Step 5: Run Bronze Ingestion

Now run your bronze ingestion in Databricks:

```python
%run /Users/faycal.arkance@gmail.com/sensor-realtime-pipeline/spark/apps/bronze_ingest.py
```

**Expected output:**
```
>>> Starting Bronze Layer ingestion...
>>> Kafka Bootstrap: 0.tcp.ngrok.io:12345
>>> Kafka Topic: sensors-data
>>> No authentication configured (using plaintext)
>>> Starting streaming query...
>>> Bronze ingestion completed successfully!
>>> Total rows in bronze_sensor_data: 157
```

---

## 🎉 SUCCESS! Full Pipeline Running

Your pipeline is now:
1. ✅ **Generator** → Producing sensor events to local Kafka
2. ✅ **Kafka** → Storing events (accessible via ngrok)
3. ✅ **Databricks** → Reading from Kafka via ngrok tunnel
4. ✅ **Bronze Table** → Storing raw events in Unity Catalog

---

## 📊 Monitor Your Pipeline

### Check Kafka Messages
```
http://localhost:8080
```

### Check Generator Logs
```bash
docker compose logs generator -f
```

### Check Bronze Data (Databricks)
```python
display(spark.table("bronze_sensor_data"))
```

---

## 🛑 Stop Everything

```bash
# Stop data generator
docker compose stop generator

# Stop all services
docker compose down

# Stop ngrok (Ctrl+C in ngrok terminal)
```

---

## 🔧 Troubleshooting

### "Connection refused" from Databricks
**Problem:** ngrok tunnel is down or wrong address
**Solution:** 
- Check ngrok is still running
- Verify the forwarding address matches your config
- Test connection: `telnet <ngrok_host> <ngrok_port>`

### "No messages in Kafka"
**Problem:** Generator not running
**Solution:**
```bash
docker compose --profile generator up -d generator
docker compose logs generator
```

### "Table bronze_sensor_data not found"
**Problem:** First run, table doesn't exist yet
**Solution:** Run bronze_ingest.py - it will create the table automatically

### Ngrok session expired (free tier limitation)
**Problem:** Free ngrok sessions expire after 2 hours
**Solution:** 
- Restart ngrok (get new address)
- Update Databricks config with new address
- Consider upgrading ngrok or use Confluent Cloud

---

## 💡 Alternative: Skip ngrok with Confluent Cloud

If ngrok is too complex, use **Confluent Cloud** instead:

1. Sign up: https://confluent.cloud/signup
2. Create Basic cluster (free tier)
3. Get credentials
4. Configure Databricks with cloud credentials
5. Stop local Docker Kafka (only keep generator if you want)

**Benefit:** No tunneling needed, Kafka is already in the cloud!

---

## 📚 Next Steps

Once Bronze is working:

1. **Run Silver layer:**
   ```python
   %run /Users/faycal.arkance@gmail.com/sensor-realtime-pipeline/spark/apps/silver_clean.py
   ```

2. **Run Gold layer:**
   ```python
   %run /Users/faycal.arkance@gmail.com/sensor-realtime-pipeline/spark/apps/gold_pipeline.py
   ```

3. **Create Databricks Jobs** to run pipeline on schedule

4. **Build Dashboards** on Gold tables

---

## 🎯 Summary

**What you have:**
- ✅ Full Kafka infrastructure in Docker
- ✅ Real sensor data generator
- ✅ ngrok tunnel for Databricks connectivity
- ✅ Bronze → Silver → Gold pipeline ready

**Total setup time:** ~10 minutes

**Enjoy your real-time sensor pipeline!** 🚀
