import sqlite3
from flask import Flask, request, jsonify
from datetime import datetime, timedelta

from typing import Dict, List

app = Flask(__name__)
DATABASE = 'elevator_data.db'

class ElevatorDataService:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_DB()
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
#iNItialize the database with required tables and views    
    def init_DB(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        schema = """
        CREATE TABLE IF NOT EXISTS buildings (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            total_floors INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS elevators (
            id INTEGER PRIMARY KEY,
            building_id INTEGER NOT NULL,
            name VARCHAR(50) NOT NULL,
            max_capacity INTEGER NOT NULL DEFAULT 10,
            min_floor INTEGER NOT NULL DEFAULT 1,
            max_floor INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (building_id) REFERENCES buildings(id)
        );

        CREATE TABLE IF NOT EXISTS demand_events (
            id INTEGER PRIMARY KEY,
            elevator_id INTEGER NOT NULL,
            requested_floor INTEGER NOT NULL,
            request_time TIMESTAMP NOT NULL,
            day_of_week INTEGER NOT NULL, -- 0 Sunday 6 Saturday
            hour_of_day INTEGER NOT NULL,  -- 0 23 not 24
            is_peak_hour BOOLEAN NOT NULL DEFAULT FALSE,
            weather_condition VARCHAR(20), --Optioinal
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (elevator_id) REFERENCES elevators(id)
        );

        CREATE TABLE IF NOT EXISTS elevator_states (
            id INTEGER PRIMARY KEY,
            elevator_id INTEGER NOT NULL,
            floor INTEGER NOT NULL,
            state VARCHAR(20) NOT NULL,
            passenger_count INTEGER DEFAULT 0,
            timestamp TIMESTAMP NOT NULL,
            previous_floor INTEGER, --last floor before change
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (elevator_id) REFERENCES elevators(id)
        );

        CREATE INDEX IF NOT EXISTS ind_demand_events_elevator_time ON demand_events(elevator_id, request_time);
        CREATE INDEX IF NOT EXISTS ind_elevator_states_elevator_time ON elevator_states(elevator_id, timestamp);
        CREATE INDEX IF NOT EXISTS ind_elevator_states_state ON elevator_states(state);
        CREATE INDEX IF NOT EXISTS ind_demand_events_peak_hour ON demand_events(is_peak_hour, hour_of_day);
        """
        
        conn.executescript(schema)
        
        conn.execute("DROP VIEW IF EXISTS ml_training_data")
        cursor.execute("""
        CREATE VIEW ml_training_data AS
        SELECT 
        es.elevator_id,
        es.floor as current_resting_floor,
        es.timestamp as rest_start_time,
        de.requested_floor as next_demand_floor,
        de.request_time as next_demand_time,
        (julianday(de.request_time) - julianday(es.timestamp)) * 24 * 60 as minutes_until_demand,
        de.day_of_week,
        de.hour_of_day,
        de.is_peak_hour,
        ABS(de.requested_floor - es.floor) as distance_to_demand,
        (SELECT COUNT(*) 
        FROM demand_events de2 
        WHERE de2.elevator_id = es.elevator_id 
        AND de2.requested_floor = de.requested_floor
        AND de2.request_time BETWEEN datetime(es.timestamp, '-7 days') AND es.timestamp
        ) as recent_demand_frequency,
        e.max_floor,
        e.min_floor
        FROM elevator_states es
        JOIN demand_events de ON de.elevator_id = es.elevator_id
        JOIN elevators e ON e.id = es.elevator_id
        WHERE es.state = 'resting'
        AND de.request_time > es.timestamp
        AND de.id = (
        SELECT MIN(de3.id) 
        FROM demand_events de3 
        WHERE de3.elevator_id = es.elevator_id 
        AND de3.request_time > es.timestamp);
        """)
#Add test data for immediate testing       
    def seed_test_data(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        #Checkk
        cursor.execute("SELECT COUNT(*) FROM buildings")
        #If already seeded, returns
        if cursor.fetchone()[0] > 0:
            conn.close()
            return
        #Add test building and elevator
        cursor.execute("""
            INSERT INTO buildings (id, name, total_floors) VALUES (1, 'Yambay Tower', 10)
        """)
        
        cursor.execute("""
            INSERT INTO elevators (id, building_id, name, min_floor, max_floor) VALUES (1, 1, 'Benitez Building', 1, 10)
        """)
        
        conn.commit()
        conn.close()
        print("Test data seeded: Building 1 with Elevator 1 (floors 1-10)")
    
  #Define peak hours based on business rules  
    def is_peak_hour(self, hour: int, day_of_week: int) -> bool:
        #Weekday morning (7-9) and evening (5-7) based on my country's business hours
        if day_of_week in [0, 1, 2, 3, 4]:#Monday [0] to Friday [4]

            return hour in [7, 8, 17, 18]
        # Weekend lunch
        elif day_of_week in [0, 6]: #Weekend
            return hour in [12, 13]#lunch
        return False
    #Saves demand events when someone calls the elevator
    def record_demand(self, elevator_id: int, requested_floor: int, request_time: datetime = None) -> Dict:
        if request_time is None:
            request_time = datetime.now()
        #0 Monday!!!
        day_of_week = request_time.weekday()  
        hour_of_day = request_time.hour
        is_peak = self.is_peak_hour(hour_of_day, day_of_week)
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO demand_events 
            (elevator_id, requested_floor, request_time, day_of_week, hour_of_day, is_peak_hour) VALUES (?, ?, ?, ?, ?, ?)
        """, (elevator_id, requested_floor, request_time, day_of_week, hour_of_day, is_peak))
        demand_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return {'demand_id': demand_id,
            'elevator_id': elevator_id,
            'requested_floor': requested_floor,
            'is_peak_hour': is_peak,
            'timestamp': request_time.isoformat()
        }
    #Saves elevator state changes when it moves or rests
    def record_elevator_state(self, elevator_id: int, floor: int, state: str, passenger_count: int = 0, previous_floor: int = None, timestamp: datetime = None) -> Dict:
        if timestamp is None:
            timestamp = datetime.now()#ojo
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        #Validate state transitions
        if state not in ['resting', 'moving', 'occupied']:
            raise ValueError(f"Invalid state: {state}")
        
        #Validate elevator bounds
        cursor.execute("SELECT min_floor, max_floor FROM elevators WHERE id = ?", (elevator_id,))
        elevator = cursor.fetchone()
        if not elevator:
            raise ValueError(f"Elevator {elevator_id} not found")
        
        if floor < elevator['min_floor'] or floor > elevator['max_floor']:
            raise ValueError(f"Floor {floor} out of bounds for elevator {elevator_id}")
        
        cursor.execute("""
            INSERT INTO elevator_states (elevator_id, floor, state, passenger_count, timestamp, previous_floor) VALUES (?, ?, ?, ?, ?, ?)
        """, (elevator_id, floor, state, passenger_count, timestamp, previous_floor))
        
        state_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return {
            'state_id': state_id,
            'elevator_id': elevator_id,
            'floor': floor,
            'state': state,
            'timestamp': timestamp.isoformat()}

#Gets ML data    
    def get_ml_training_data(self, elevator_id: int = None, start_date: datetime = None, end_date: datetime = None) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = "SELECT * FROM ml_training_data WHERE 1=1"
        params = []
        if elevator_id:
            query += " AND elevator_id = ?"
            params.append(elevator_id)
        if start_date:
            query += " AND rest_start_time >= ?"
            params.append(start_date.strftime('%Y-%m-%d %H:%M:%S'))

        if end_date:
            query += " AND rest_start_time <= ?"
            params.append(end_date.strftime('%Y-%m-%d %H:%M:%S'))

        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def get_demand_analytics(self, elevator_id: int, days: int = 7) -> Dict:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        start_date = datetime.now() - timedelta(days=days)
        
    #floor popularity
        cursor.execute("""
            SELECT requested_floor, COUNT(*) as demand_count
            FROM demand_events 
            WHERE elevator_id = ? 
            AND request_time >= ?
            GROUP BY requested_floor
            ORDER BY demand_count DESC
        """, (elevator_id, start_date))
        
        floor_popularity = [dict(row) for row in cursor.fetchall()]
        

        cursor.execute("""
            SELECT 
            is_peak_hour, 
            AVG(CAST(hour_of_day AS FLOAT)) as avg_hour,
            COUNT(*) as total_demands
            FROM demand_events 
            WHERE elevator_id = ? 
            AND request_time >= ?
            GROUP BY is_peak_hour
        """, (elevator_id, start_date))
        
        peak_analysis = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return {'elevator_id': elevator_id,
            'analysis_period_days': days,
            'floor_popularity': floor_popularity,
            'peak_hour_analysis': peak_analysis}




service = ElevatorDataService(DATABASE)

#Endpoints,
#Saves demand
@app.route('/elevators/<int:elevator_id>/demand', methods=['POST'])
def record_demand(elevator_id):
    data = request.get_json()
    if 'requested_floor' not in data:
        return jsonify({'error': 'requested_floor is required'}), 400
    
    try:
        request_time = None
        if 'request_time' in data:
            request_time = datetime.fromisoformat(data['request_time'])
        result = service.record_demand(
            elevator_id=elevator_id,
            requested_floor=data['requested_floor'],
            request_time=request_time
        )
        return jsonify(result), 201
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400
#Saves elevator state
@app.route('/elevators/<int:elevator_id>/state', methods=['POST'])
def record_state(elevator_id):
    """Record elevator state change"""
    data = request.get_json()
    
    required_fields = ['floor', 'state']
    for field in required_fields:
        if field not in data:
            return jsonify({'error': f'{field} is required'}), 400
    
    try:
        result = service.record_elevator_state(
            elevator_id=elevator_id,
            floor=data['floor'],
            state=data['state'],
            passenger_count=data.get('passenger_count', 0),
            previous_floor=data.get('previous_floor')
        )
        return jsonify(result), 201
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400
#Brings training data
@app.route('/training-data', methods=['GET'])
def get_training_data():
    elevator_id = request.args.get('elevator_id', type=int)

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')    
    try:
        start = datetime.fromisoformat(start_date) if start_date else None
        end = datetime.fromisoformat(end_date) if end_date else None
        
        data = service.get_ml_training_data(elevator_id, start, end)
        
        return jsonify({'count': len(data),'data': data})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

#Brings demand analytics
@app.route('/elevators/<int:elevator_id>/analytics', methods=['GET'])
def get_analytics(elevator_id):
    days = request.args.get('days', default=7, type=int)
    try:
        analytics = service.get_demand_analytics(elevator_id, days)
        return jsonify(analytics)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
#Health check, classic
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    app.run(debug=True, port=2025)