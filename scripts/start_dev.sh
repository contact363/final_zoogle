#!/bin/bash
# Start Zoogle in development mode

echo "Starting Zoogle Development Environment..."

# Start PostgreSQL + Redis via Docker
docker-compose up -d postgres redis

echo "Waiting for databases..."
sleep 5

# Backend (FastAPI)
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000 &
BACKEND_PID=$!

# Celery worker
celery -A tasks.celery_app worker --loglevel=info &
CELERY_PID=$!

# Frontend (Next.js)
cd ../frontend
npm install
npm run dev &
FRONTEND_PID=$!

echo ""
echo "Zoogle is running:"
echo "  Frontend:  http://localhost:3000"
echo "  Backend:   http://localhost:8000"
echo "  API Docs:  http://localhost:8000/docs"
echo "  Admin:     http://localhost:3000/admin"
echo "  Flower:    http://localhost:5555"
echo ""
echo "Press Ctrl+C to stop all services"

trap "kill $BACKEND_PID $CELERY_PID $FRONTEND_PID 2>/dev/null" EXIT
wait
