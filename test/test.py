import sqlite3
import tempfile
import pytest

import os
from datetime import datetime, timedelta
from unittest.mock import patch

import json

import sys
sys.path.append('.')
from app.elevator_api import ElevatorDataService, app
#Creates a temp DB
class TestElevatorDataService:
    @pytest.fixture
    def service(self):
        db_fd, db_path = tempfile.mkstemp()#
        os.close(db_fd)
        
        # Create test schema
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE elevators (
                id INTEGER PRIMARY KEY,
                building_id INTEGER NOT NULL,
                name VARCHAR(50) NOT NULL,
                max_capacity INTEGER NOT NULL DEFAULT 10,
                min_floor INTEGER NOT NULL DEFAULT 1,
                max_floor INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE demand_events (
                id INTEGER PRIMARY KEY,
                elevator_id INTEGER NOT NULL,
                requested_floor INTEGER NOT NULL,
                request_time TIMESTAMP NOT NULL,
                day_of_week INTEGER NOT NULL,
                hour_of_day INTEGER NOT NULL,
                is_peak_hour BOOLEAN NOT NULL DEFAULT FALSE,
                weather_condition VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE elevator_states (
                id INTEGER PRIMARY KEY,
                elevator_id INTEGER NOT NULL,
                floor INTEGER NOT NULL,
                state VARCHAR(20) NOT NULL,
                passenger_count INTEGER DEFAULT 0,
                timestamp TIMESTAMP NOT NULL,
                previous_floor INTEGER
            );

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
                10 as max_floor,
                1 as min_floor
            FROM elevator_states es
            JOIN demand_events de ON de.elevator_id = es.elevator_id
            WHERE es.state = 'resting'
            AND de.request_time > es.timestamp
            AND de.id = (
                SELECT MIN(de3.id) 
                FROM demand_events de3 
                WHERE de3.elevator_id = es.elevator_id 
                AND de3.request_time > es.timestamp
            );
            
            -- Test data
            INSERT INTO elevators (id, building_id, name, min_floor, max_floor) 
            VALUES (1, 1, 'Main Elevator', 1, 10);
        """)
        conn.commit()
        conn.close()
        
        service = ElevatorDataService(db_path)
        yield service
       
        os.unlink(db_path)
#Tests if "peak hours" works    
    def test_peak_hour_detection(self, service):
        #Weekday peak
        assert service.is_peak_hour(8, 1) == True#Tuesday 8am
        assert service.is_peak_hour(17, 4) == True#Friday 17
        #Not peak
        assert service.is_peak_hour(10, 2) == False #Wedn 10
        assert service.is_peak_hour(20, 3) == False#Thursday 20
        #Weekend peak hour
        assert service.is_peak_hour(12, 6) == True
        assert service.is_peak_hour(8, 6) == False
    #Test demand
    def test_record_demand_success(self, service):
        test_time = datetime(2025, 1, 15, 8, 30)#Monday 8.30
        
        result = service.record_demand(elevator_id=1, requested_floor=5,request_time=test_time)        
        assert result['elevator_id'] == 1
        assert result['requested_floor'] == 5
        assert result['is_peak_hour'] == True#Should be peak!!! True
        assert 'demand_id' in result
    #STate success
    def test_record_elevator_state_success(self, service):
        result = service.record_elevator_state(elevator_id=1, floor=3,state='resting',passenger_count=0,previous_floor=2)        
        assert result['elevator_id'] == 1
        assert result['floor'] == 3
        assert result['state'] == 'resting'
        assert 'state_id' in result
#Validation tests for states
    def test_elevator_state_validation(self, service):
        #Not a valid state!
        with pytest.raises(ValueError, match="Invalid state"):
            service.record_elevator_state(1, 3, 'invalid_state')
        
        #Out of bounds floor numbers
        with pytest.raises(ValueError, match="out of bounds"):
            service.record_elevator_state(1, 15, 'resting')#max  10
        #
        with pytest.raises(ValueError, match="out of bounds"):
            service.record_elevator_state(1, 0, 'resting')#min 1
#Test ML Format    
    def test_ml_training_data_format(self, service):
        #elevator rests on floor 3, then demand comes for floor 7
        rest_time = datetime(2025, 1, 15, 8, 0)
        demand_time = datetime(2025, 1, 15, 8, 5)#5min diff
        
        #saves resting state
        service.record_elevator_state(1, 3, 'resting', timestamp=rest_time)
        #demand 5 minutes later
        service.record_demand(1, 7, demand_time)
        
        #Gets training data
        training_data = service.get_ml_training_data(elevator_id=1)
        
        assert len(training_data) == 1# Should have one record
        record = training_data[0]
        
        # Check key ML features
        assert record['current_resting_floor'] == 3
        assert record['next_demand_floor'] == 7
        assert record['distance_to_demand'] == 4  #from 3 to 7 there are 4 levels
        assert record['is_peak_hour'] == 1#True
        assert abs(record['minutes_until_demand'] - 5) < 1##5 minutes until demand


    def test_demand_analytics(self, service):
        floors = [1, 2, 2, 3, 3, 3]#Floor 3 most popular (3 times)
        for floor in floors:
            service.record_demand(1, floor)#Saves every floor for elevator 1
        
        analytics = service.get_demand_analytics(1, days=1)
        
        assert analytics['elevator_id'] == 1
        assert len(analytics['floor_popularity']) == 3
        #"Most popular floor should be first"
        most_popular = analytics['floor_popularity'][0]
        assert most_popular['requested_floor'] == 3
        assert most_popular['demand_count'] == 3
#Tests for filtering data by date    
    def test_data_filtering_by_date(self, service):
        old_time = datetime(2025, 1, 1, 10, 0)
        recent_time = datetime(2025, 1, 15, 10, 0)
        #Old
        service.record_elevator_state(1, 2, 'resting', timestamp=old_time)
        service.record_demand(1, 5, old_time + timedelta(seconds=1))
        #Reent  
        service.record_elevator_state(1, 4, 'resting', timestamp=recent_time)
        service.record_demand(1, 8, recent_time + timedelta(seconds=1))
        #Filter for recent data only
        start_date = datetime(2025, 1, 10)
        training_data = service.get_ml_training_data(elevator_id=1, start_date=start_date)
        
        #Should only get recent
        assert len(training_data) == 1
        assert training_data[0]['next_demand_floor'] == 8



class TestAPIEndpoints:
    @pytest.fixture
    def client(self):#Creates test client
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client
    
    @patch('app.elevator_api.service')
    def test_record_demand_endpoint(self, mock_service, client):
        mock_service.record_demand.return_value = {'demand_id': 1, 'elevator_id': 1, 
                                                   'requested_floor': 5,'is_peak_hour': True,
            'timestamp': '2025-01-15T08:30:00'}
        
        response = client.post('/elevators/1/demand', 
                             json={'requested_floor': 5})
        
        assert response.status_code == 201
        data = json.loads(response.data)
        assert data['requested_floor'] == 5
        assert data['elevator_id'] == 1
    
    @patch('app.elevator_api.service')
    def test_record_demand_validation(self, mock_service, client):
        #Missing required field, should get error
        response = client.post('/elevators/1/demand', json={})
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert 'requested_floor is required' in data['error']####here
    
    @patch('app.elevator_api.service')
    def test_record_state_endpoint(self, mock_service, client):#State recording
        mock_service.record_elevator_state.return_value = {'state_id': 1, 'elevator_id': 1, 'floor': 3,
                                                           'state': 'resting','timestamp': '2025-01-15T08:30:00'}
        
        response = client.post('/elevators/1/state', json={'floor': 3, 'state': 'resting'})
        
        assert response.status_code == 201
        data = json.loads(response.data)
        assert data['floor'] == 3
        assert data['state'] == 'resting'
    #Test ML training data endopint
    @patch('app.elevator_api.service')
    def test_training_data_endpoint(self, mock_service, client):
        mock_service.get_ml_training_data.return_value = [{'current_resting_floor': 3,
                'next_demand_floor': 7, 'distance_to_demand': 4, 'is_peak_hour': 1,'minutes_until_demand': 5.2}]
        response = client.get('/training-data?elevator_id=1')
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['count'] == 1
        assert len(data['data']) == 1
        assert data['data'][0]['distance_to_demand'] == 4
#health check endpoint    
    def test_health_check(self, client):
        response = client.get('/health')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['status'] == 'healthy'
        assert 'timestamp' in data
#Tests data integrity
class TestDataIntegrity:
    @pytest.fixture
    def service(self):
        db_fd, db_path = tempfile.mkstemp()
        os.close(db_fd)
        try:
            conn = sqlite3.connect(db_path)
            #Creates test schema with minimal data
            conn.executescript("""
                CREATE TABLE elevators (
                    id INTEGER PRIMARY KEY,
                    building_id INTEGER NOT NULL,
                    name VARCHAR(50) NOT NULL,
                    min_floor INTEGER NOT NULL DEFAULT 1,
                    max_floor INTEGER NOT NULL
                );
                
                CREATE TABLE demand_events (
                    id INTEGER PRIMARY KEY,
                    elevator_id INTEGER NOT NULL,
                    requested_floor INTEGER NOT NULL,
                    request_time TIMESTAMP NOT NULL,
                    day_of_week INTEGER NOT NULL,
                    hour_of_day INTEGER NOT NULL,
                    is_peak_hour BOOLEAN NOT NULL DEFAULT FALSE
                );
                
                CREATE TABLE elevator_states (
                    id INTEGER PRIMARY KEY,
                    elevator_id INTEGER NOT NULL,
                    floor INTEGER NOT NULL,
                    state VARCHAR(20) NOT NULL,
                    passenger_count INTEGER DEFAULT 0,
                    timestamp TIMESTAMP NOT NULL,
                    previous_floor INTEGER
                );
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
                    10 as max_floor,
                    1 as min_floor
                FROM elevator_states es
                JOIN demand_events de ON de.elevator_id = es.elevator_id
                WHERE es.state = 'resting'
                AND de.request_time > es.timestamp
                AND de.id = (
                    SELECT MIN(de3.id) 
                    FROM demand_events de3 
                    WHERE de3.elevator_id = es.elevator_id 
                    AND de3.request_time > es.timestamp
                );
                            
                INSERT INTO elevators VALUES (1, 1, 'Test', 1, 10);
                
            """)
            conn.commit()
            conn.close()
            
            service = ElevatorDataService(db_path)
            yield service
        finally:#Cleanup temp DB
            if os.path.exists(db_path):
                os.unlink(db_path)
#Concurrent demand recording test    
    def test_concurrent_demand_recording(self, service):
        demands = []
        for i in range(10):
            result = service.record_demand(1, i % 10 + 1)
            demands.append(result['demand_id'])
        assert len(set(demands)) == 10#unique ids
    #Record sequence of states
    def test_state_transition_logging(self, service):
        states =[(1, 'resting'), (3, 'moving'),(5, 'occupied'), (5, 'resting')]
        
        state_ids = []
        for floor, state in states:#Record each state
            result = service.record_elevator_state(1, floor, state)
            state_ids.append(result['state_id'])
        
        # Check all states recorded
        conn = service.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM elevator_states WHERE elevator_id = 1")
        count = cursor.fetchone()[0]
        conn.close()
        
        assert count == 4

if __name__ == '__main__':
    pytest.main(['-v', __file__])#-v to show verbose output