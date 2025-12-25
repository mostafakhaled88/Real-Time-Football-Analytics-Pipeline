import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --------------------
# Configuration
# --------------------
# Use localhost:29092 for connection from your host machine to Docker
KAFKA_BROKER = "localhost:29092"
TOPIC = "football-events"

# Mock Match Data
MATCH_ID = "PL-2024-001"
TEAM_A = "Arsenal"
TEAM_B = "Manchester City"
PLAYERS = {
    TEAM_A: ["Saka", "Odegaard", "Rice", "Havertz", "Saliba"],
    TEAM_B: ["Haaland", "De Bruyne", "Foden", "Rodri", "Bernardo"]
}

# Event distribution: 70% Pass, 10% Shot, 10% Foul, 5% Goal, 5% Card
EVENT_TYPES = ["PASS", "SHOT", "FOUL", "GOAL", "CARD"]
EVENT_WEIGHTS = [0.70, 0.10, 0.10, 0.05, 0.05]

def get_producer():
    """Initializes the Kafka Producer with reliability settings."""
    try:
        return KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # acks=1 ensures the broker receives the message before moving on
            acks=1,
            # Retries in case of transient network issues
            retries=5
        )
    except KafkaError as e:
        print(f"Failed to create producer: {e}")
        return None

def generate_event():
    """Generates a random football match event."""
    team = random.choice([TEAM_A, TEAM_B])
    player = random.choice(PLAYERS[team])
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    
    event = {
        "event_id": f"evt_{int(time.time() * 1000)}",
        "match_id": MATCH_ID,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "team": team,
        "player": player,
        "event_type": event_type,
        "location": {
            "x": round(random.uniform(0, 100), 2),
            "y": round(random.uniform(0, 100), 2)
        },
        "details": "N/A"
    }
    
    # Logic for specific events
    if event_type == "CARD":
        event["details"] = random.choice(["Yellow", "Red"])
    elif event_type == "SHOT":
        event["details"] = random.choice(["On Target", "Off Target", "Blocked"])
        
    return event

if __name__ == "__main__":
    producer = get_producer()
    
    if producer:
        print(f"--- Connection Established ---")
        print(f"Starting Live Stream: {TEAM_A} vs {TEAM_B}...")
        print(f"Viewing on: http://localhost:8085 (Kafka UI)")
        print("-" * 40)
        
        try:
            while True:
                data = generate_event()
                
                # Send data to Kafka
                producer.send(TOPIC, data)
                
                # Force the message to be sent immediately rather than waiting for a batch
                producer.flush()
                
                print(f"[{data['timestamp']}] {data['team']} | {data['player']} | {data['event_type']}")
                
                # Simulate match speed
                time.sleep(random.uniform(1, 3))
                
        except KeyboardInterrupt:
            print("\nMatch Ended by User.")
        except Exception as e:
            print(f"\nError during streaming: {e}")
        finally:
            producer.close()
            print("Producer connection closed.")
    else:
        print("Could not start producer. Ensure your Docker containers are running.")
