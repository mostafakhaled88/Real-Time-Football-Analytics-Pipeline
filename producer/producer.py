import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# --------------------
# Configuration
# --------------------
KAFKA_BROKER = "localhost:29092"
TOPIC = "football-events"

# Mock Data
TEAMS = ["Red Dragons", "Blue Titans"]
EVENT_TYPES = ["PASS", "SHOT", "FOUL", "CORNER", "GOAL", "YELLOW_CARD"]
PLAYERS = {
    "Red Dragons": ["D. Smith", "J. Doe", "A. Brown", "M. Wilson"],
    "Blue Titans": ["L. Messi", "C. Ronaldo", "K. Mbappe", "N. Neymar"]
}

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# Initialize Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=json_serializer
)

print(f"Starting producer on topic: {TOPIC}...")

def generate_event():
    team = random.choice(TEAMS)
    player = random.choice(PLAYERS[team])
    # Weighting events so 'PASS' happens more often than 'GOAL'
    event_type = random.choices(
        EVENT_TYPES, 
        weights=[60, 15, 10, 8, 2, 5], 
        k=1
    )[0]
    
    return {
        "timestamp": datetime.now().isoformat(),
        "team": team,
        "player": player,
        "event_type": event_type,
        "match_id": "MATCH_2025_001",
        "location": {
            "x": random.randint(0, 100),
            "y": random.randint(0, 100)
        }
    }

try:
    while True:
        event = generate_event()
        producer.send(TOPIC, event)
        print(f"Sent: {event['team']} - {event['player']}: {event['event_type']}")
        
        # Sleep for a random interval to simulate real-game rhythm
        time.sleep(random.uniform(0.5, 3.0))
except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.flush()
    producer.close()