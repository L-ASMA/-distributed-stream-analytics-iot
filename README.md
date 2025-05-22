# -distributed-stream-analytics-iot
##  Prerequisites

Make sure the following tools are installed:

* Java JDK 11+
*  Apache Maven
*  Docker & Docker Compose
*  Node.js + npm (for React Dashboard)
*  
##  Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/smart-city-platform.git
cd smart-city-platform
```

### 2. Start Infrastructure

```bash
docker-compose up -d
```

This command launches:

| Service    | Description                  | Port |
| ---------- | ---------------------------- | ---- |
| Kafka      | Message broker               | 9092 |
| Zookeeper  | Kafka coordination service   | 2181 |
| PostgreSQL | Database for structured data | 5432 |
| Grafana    | Visualization & alerting     | 3000 |
| Kafdrop    | Kafka UI Dashboard           | 9000 |

---

### 3. Run Kafka Producers

Each producer simulates sensor data:

```bash
cd producers/water-producer
mvn clean package
java -jar target/water-producer.jar
```

Do the same for:

* `crime-producer`
* `air-quality-producer`

---

### 4. Run Kafka Consumers

Each consumer stores Kafka data in PostgreSQL:

```bash
cd consumers/water-consumer
mvn clean package
java -jar target/water-consumer.jar
```

Repeat for each dataset.

---

##  Visualizing in Grafana

1. Open Grafana: [http://localhost:3000](http://localhost:3000)

   * Default login: `admin` / `admin`

2. Add PostgreSQL Data Source

   * Host: `postgres:5432`
   * DB: `smartcity`, User: `admin`, Password: `admin`

3. Create or import dashboards:

   * Water flow by zone
   * Crime type distribution
   * Air quality metrics

4. Configure alerts from panel → alert tab → **"Add Alert Rule"**.

---

##  Live Dashboard (React + WebSocket)

### 5. Run WebSocket Server

```bash
cd websocket-server
mvn clean package
java -jar target/websocket-server.war
```

This exposes WebSocket endpoints like:

* `ws://localhost:8080/websocket-server/ws/crimes`
* `ws://localhost:8080/websocket-server/ws/water`

---

### 6. Run React Frontend Dashboard

```bash
cd dashboard-frontend
npm install
npm run dev
```

Accessible at [http://localhost:5173](http://localhost:5173)

---

##  Directory Structure

smart-city-platform/
├── docker-compose.yml
├── producers/
│   ├── water-producer/
│   ├── crime-producer/
│   └── air-quality-producer/
├── consumers/
│   ├── water-consumer/
│   ├── crime-consumer/
│   └── air-quality-consumer/
├── websocket-server/
├── dashboard-frontend/     # React client
├── grafana/                # Predefined dashboards
├── data/                   # CSV datasets
└── README.md
```

---

##  Credentials

| Service    | Username | Password |
| ---------- | -------- | -------- |
| PostgreSQL | admin    | admin    |
| Grafana    | admin    | admin    |

---

##  Features Recap

*  Real-time Kafka pipeline
*  PostgreSQL storage with typed schema
*  Grafana alerts & dashboards
*  Kafdrop Kafka monitoring UI
*  WebSocket live data push to React
*  Docker-based deployment

## Author

**ASMAE LAMGARI**, **OUSSEF OUAZIZE**
