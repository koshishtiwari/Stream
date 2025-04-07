"""IoT Sensor Data Source Implementation"""
import json
import random
import time
import logging
from datetime import datetime
from typing import Dict, Generator, List, Any

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IoTSensor:
    """Base class for IoT sensors"""
    def __init__(self, sensor_id: str, sensor_type: str, location: str):
        """
        Initialize an IoT sensor
        
        Args:
            sensor_id: Unique identifier for the sensor
            sensor_type: Type of sensor (e.g., 'temperature', 'humidity')
            location: Physical location of the sensor
        """
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.location = location
        logger.debug(f"Initialized {sensor_type} sensor {sensor_id} at {location}")
        
    def generate_reading(self) -> Dict[str, Any]:
        """Generate a sensor reading"""
        timestamp = datetime.now().isoformat()
        return {
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "location": self.location,
            "timestamp": timestamp,
        }

class TemperatureSensor(IoTSensor):
    """Temperature sensor implementation"""
    def __init__(self, sensor_id: str, location: str, min_temp: float = 15.0, max_temp: float = 35.0):
        """
        Initialize a temperature sensor
        
        Args:
            sensor_id: Unique identifier for the sensor
            location: Physical location of the sensor
            min_temp: Minimum temperature in Celsius
            max_temp: Maximum temperature in Celsius
        """
        super().__init__(sensor_id, "temperature", location)
        self.min_temp = min_temp
        self.max_temp = max_temp
        self.last_temp = (min_temp + max_temp) / 2
        logger.debug(f"Temperature sensor range: {min_temp}°C - {max_temp}°C")
        
    def generate_reading(self) -> Dict[str, Any]:
        """Generate a temperature reading with slight variations"""
        base_reading = super().generate_reading()
        # Simulate temperature with some random variation
        variation = random.uniform(-0.5, 0.5)
        self.last_temp += variation
        
        # Keep temperature within specified range
        if self.last_temp > self.max_temp:
            self.last_temp = self.max_temp
        elif self.last_temp < self.min_temp:
            self.last_temp = self.min_temp
            
        base_reading["value"] = round(self.last_temp, 2)
        base_reading["unit"] = "celsius"
        return base_reading

class IoTDataSource:
    """IoT Data source that aggregates multiple sensors and provides data stream"""
    def __init__(self, sensors: List[IoTSensor]):
        """
        Initialize an IoT data source with a list of sensors
        
        Args:
            sensors: List of IoT sensors to stream data from
        """
        self.sensors = sensors
        logger.info(f"IoT data source initialized with {len(sensors)} sensors")
        
    def stream(self, interval: float = 1.0) -> Generator[Dict[str, Any], None, None]:
        """
        Stream data from all sensors at specified interval
        
        Args:
            interval: Time between readings in seconds
            
        Yields:
            Dict containing sensor data
        """
        while True:
            for sensor in self.sensors:
                yield sensor.generate_reading()
                time.sleep(interval / len(self.sensors))

# Example usage
if __name__ == "__main__":
    # Create some sample sensors
    temp_sensor1 = TemperatureSensor("temp-001", "datacenter-a")
    temp_sensor2 = TemperatureSensor("temp-002", "datacenter-b", min_temp=10.0, max_temp=30.0)
    
    # Create data source with these sensors
    data_source = IoTDataSource([temp_sensor1, temp_sensor2])
    
    # Stream and print 10 readings
    for i, reading in enumerate(data_source.stream(interval=0.5)):
        print(json.dumps(reading))
        if i >= 9:
            break
