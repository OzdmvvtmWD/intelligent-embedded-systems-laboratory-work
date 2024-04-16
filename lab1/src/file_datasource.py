import config
from csv import reader
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
from domain.aggregated_data import AggregatedData
from datetime import datetime


class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        
    def read(self) -> AggregatedData:
        x,y,z = self.ax.__next__()
        try:
           lg, lt = self.gps.__next__()
        except StopIteration:
           self.gps_file.seek(0,0)
           lg, lt = self.gps.__next__()
           lg, lt = self.gps.__next__()
           
        p = self.park.__next__()[0]
        return AggregatedData(
            Accelerometer(x, y, z),
            Gps(lg, lt),
            Parking(p, Gps(lg, lt)),
            datetime.now(),
            config.USER_ID,
        )
    def initialize_reader(self,filename):
        file = open(filename, newline='')
        reader_obj = reader(file, delimiter=',', quotechar='|')
        next(reader_obj)  
        return file, reader_obj


    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        self.accelerometer_file, self.ax = self.initialize_reader(self.accelerometer_filename)
        self.gps_file, self.gps = self.initialize_reader(self.gps_filename)
        self.parking_file, self.park = self.initialize_reader(self.parking_filename)
           
              
    def stopReading(self, *args, **kwargs):
        self.accelerometer_file.close()
        self.gps_file.close()
        self.parking_file.close()
        
