from datetime import datetime, timedelta
from multiprocessing import Process
from abc import ABC, abstractmethod

def simulation_timer(start_datetime, acceleration):
    real_start_datetime = datetime.now()
    while True:
        simulated_datetime = (datetime.now()-real_start_datetime)*acceleration+start_datetime
        print('Simulated time', simulated_datetime)
        sleep(1)


class CrimeStreamSimulator(ABC):
    def __init__(self, crime_batch_path, streamer, start_datetime,
                        acceleration=1, nb_officer=1,
                        crime_of_interest=('ASSAULT', 'BURGLARY', 'ROBBERY'),
                        report_time_range=(30, 90)):
        """ An abstract class to simulates a streamer/producer for crime

        It will:
        1. Filter the crime of interest (`ASSAULT`, `BURGLARY`, `ROBBERY`)
        2. Simulate the report time (30~90 minutes after it commited)
        3. Sort and Partition all records into one or many parts randomly.
            Each partition represents a police officer

        Parameters:
            crime_batch_path: the path of the stored
            streamer: the function used to stream
            start_datetime: the simuation start since when
            acceleration: how fast the simulated time pass
            nb_officer: number of officer(s)
            crime_of_interest: will only keep crimes of those types 
            report_time_range: the range of duration between occcur time and report time in minutes

        Abstract Methods:
            _load_batch: load batch (as a Pandas object/a RDD object/a list of buffers...)
            _filter: only keep crime of interest
            _simulate_report_time: simulate the report time
            _partition: partition all data
            _assign: assign partitions and informations to each officers
            _cleanup: tear down the preprocessing environment 

        Officer definition:
            Should have a method `start` to start streaming asynchronously,
                and have a method `join` when it ends. You can use class 
                `Process` from `multiprocessing` directly.
        """

        # load batch file
        print('Loading batch...')
        self._load_batch(crime_batch_path)
        # keep crime_of_interest 
        print('Filtering batch...')
        self._filter(crime_of_interest)
        # simulate the report time
        print('Generating report time...')
        self._simulate_report_time(report_time_range, start_datetime)
        # parition
        print('Partitioning the data...')
        self.officers = self._partition(nb_officer)
        # assign 
        self.officers = self._assign(start_datetime, acceleration, streamer)
        # cleanup
        self._cleanup()
        self.timer = Process(target=simulation_timer, args=(start_datetime, acceleration))

    def start(self):
        # start all processors
        for officers in self.officers:
            officers.start()
        self.timer.start()

        # wait all processors to end
        for officers in self.officers:
            officers.join()
        self.timer.terminate()

    @abstractmethod
    def _load_batch(self, path):
        raise NotImplementedError

    @abstractmethod
    def _filter(self, crime_of_interest):
        raise NotImplementedError

    @abstractmethod
    def _simulate_report_time(self, report_time_range):
        raise NotImplementedError

    @abstractmethod
    def _partition(self, nb_partition):
        raise NotImplementedError

    @abstractmethod
    def _assign(self, start_datetime, acceleration, streamer):
        raise NotImplementedError

    @abstractmethod
    def _cleanup(self):
        raise NotImplementedError


import pandas as pd
import numpy as np
from time import sleep


class BasicCrimeStreamSimulator(CrimeStreamSimulator):
    """ A implementation of CrimeStreamer with Pandas and multiprocessing, assuming that the batch file can be loaded in the memory.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _load_batch(self, path):
        self._batch = pd.read_csv(path)

    def _filter(self, crime_of_interest):
        self._batch.dropna(axis=0, subset=['Crime Code Description'], inplace=True)
        self._batch = self._batch[self._batch['Crime Code Description'].str.contains('|'.join(crime_of_interest))]

    def _simulate_report_time(self, report_time_range, start_datetime):
        def generate_report_time(row):
            month, day, year = map(int, row['Date Occurred'].split('/'))
            hour, minute = row['Time Occurred']//100, row['Time Occurred']%100
            occ_time = datetime(year, month, day, hour, minute)
            rep_time = occ_time+timedelta(minutes=np.random.randint(*report_time_range))
            return rep_time
        self._batch['Stream Time'] = self._batch.apply(generate_report_time, axis=1)

        # filter out those before start time
        self._batch = self._batch[self._batch['Stream Time'] > start_datetime]

    def _partition(self, nb_partition):
        # shuffle 
        batch = self._batch.sample(frac=1).reset_index(drop=True)
        # split
        partitions = np.array_split(batch, nb_partition)
        # sort 
        partitions = [par.sort_values('Stream Time') for par in partitions]
        self._partitions = partitions

    def _assign(self, start_datetime, acceleration, streamer):
        # create process
        officers = [Process(target=streamer, args=(ind, par, start_datetime, acceleration))
                for ind, par in enumerate(self._partitions)]
        return officers

    def _cleanup(self):
        del self._batch
        del self._partitions


def printing_streamer(pid, data, start_datetime, acceleration):
    real_start_datetime = datetime.now()
    simulated_datetime = (datetime.now()-real_start_datetime)*acceleration+start_datetime
    for ind, datum in data.iterrows():
        while simulated_datetime < datum['Stream Time']:
            sleep(1)
            simulated_datetime = (datetime.now()-real_start_datetime)*acceleration+start_datetime
        print('Officer', pid, 'reported', datum.to_json())


def kafka_streamer(pid, data, start_datetime, acceleration):
    """
        TODO: redesign streamer
                use decorator/class wrapper (factory pattern)
                or inherit from multiprocess.Process
    """
    from pykafka import KafkaClient
    client = KafkaClient(hosts="localhost:9092,localhost:9093,localhost:9094")
    topic = client.topics['la-crime']

    with topic.get_sync_producer() as producer:
        real_start_datetime = datetime.now()
        simulated_datetime = (datetime.now()-real_start_datetime)*acceleration+start_datetime
        for ind, datum in data.iterrows():
            while simulated_datetime < datum['Stream Time']:
                sleep(1)
                simulated_datetime = (datetime.now()-real_start_datetime)*acceleration+start_datetime
            datum['Officer Id'] = pid
            producer.produce( datum.to_json().encode() )


if __name__ == '__main__':
    streamer = BasicCrimeStreamSimulator(
            crime_batch_path='data/crime.csv',
            # crime_batch_path='data/crime_small.csv',
            # streamer=printing_streamer,
            streamer=kafka_streamer,
            start_datetime=datetime(2013, 12, 1),
            acceleration=60*60,  # 1 second in simulation is 3*60*60 seconds
            nb_officer=4
    )
    streamer.start()
