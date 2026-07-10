#!/bin/bash
# Quick Start Script - Local Kafka with Docker + ngrok
# Run this from your project root directory

set -e

echo "=========================================="
echo "🚀 STARTING LOCAL KAFKA INFRASTRUCTURE"
echo "=========================================="

# Step 1: Start Docker services
echo ""
echo "📦 Step 1/4: Starting Docker services..."
docker compose up -d

echo ""
echo "⏳ Waiting for services to be healthy (30 seconds)..."
sleep 30

echo ""
echo "✅ Docker services status:"
docker compose ps

# Step 2: Start data generator
echo ""
echo "=========================================="
echo "🎲 Step 2/4: Starting Data Generator"
echo "=========================================="
docker compose --profile generator up -d generator

echo ""
echo "✅ Generator logs (last 10 lines):"
docker compose logs generator | tail -10

# Step 3: Instructions for ngrok
echo ""
echo "=========================================="
echo "🌐 Step 3/4: MANUAL - Expose Kafka with ngrok"
echo "=========================================="
echo ""
echo "In a NEW TERMINAL, run:"
echo ""
echo "    ngrok tcp 29092"
echo ""
echo "⚠️  IMPORTANT: Keep that terminal open!"
echo "⚠️  Note the forwarding address, example:"
echo "    tcp://0.tcp.ngrok.io:12345 -> localhost:29092"
echo ""
echo "Press ENTER when ngrok is running and you have noted the address..."
read

# Step 4: Configuration instructions
echo ""
echo "=========================================="
echo "⚙️  Step 4/4: Configure Databricks"
echo "=========================================="
echo ""
echo "Copy and run this in Databricks (replace with YOUR ngrok address):"
echo ""
echo "import os"
echo "os.environ[\"KAFKA_BOOTSTRAP\"] = \"0.tcp.ngrok.io:12345\"  # REPLACE with your ngrok address"
echo "os.environ[\"KAFKA_TOPIC\"] = \"sensors-data\""
echo "print(\"✅ Kafka configured!\")"
echo ""
echo "Then run:"
echo "%run /Users/faycal.arkance@gmail.com/sensor-realtime-pipeline/spark/apps/bronze_ingest.py"
echo ""

# Summary
echo ""
echo "=========================================="
echo "✅ LOCAL KAFKA INFRASTRUCTURE READY!"
echo "=========================================="
echo ""
echo "Services running:"
echo "  ✅ Kafka:      localhost:29092 (exposed via ngrok)"
echo "  ✅ Kafka UI:   http://localhost:8080"
echo "  ✅ Generator:  Sending events to sensors-data topic"
echo "  ✅ Postgres:   localhost:5432"
echo "  ✅ Metabase:   http://localhost:3000"
echo ""
echo "Next steps:"
echo "  1. Configure Databricks (see above)"
echo "  2. Run bronze_ingest.py in Databricks"
echo "  3. Monitor at http://localhost:8080"
echo ""
echo "To stop everything:"
echo "  docker compose down"
echo "  (Ctrl+C in ngrok terminal)"
echo ""
